/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.skp.experiment.cf.als.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.impl.common.FullRunningAverage;
import org.apache.mahout.cf.taste.impl.common.RunningAverage;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.mapreduce.MergeVectorsCombiner;
import org.apache.mahout.common.mapreduce.MergeVectorsReducer;
import org.apache.mahout.common.mapreduce.TransposeMapper;
import org.apache.mahout.common.mapreduce.VectorSumReducer;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.als.AlternatingLeastSquaresSolver;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.skp.experiment.math.als.hadoop.DistributedImplicitFeedbackAlternatingLeastSquaresSolver;
import com.skp.experiment.math.als.hadoop.ImplicitFeedbackAlternatingLeastSquaresSolver;
/**
 * <p>MapReduce implementation of the two factorization algorithms described in
 *
 * <p>"Large-scale Parallel Collaborative Filtering for the Netﬂix Prize" available at
 * http://www.hpl.hp.com/personal/Robert_Schreiber/papers/2008%20AAIM%20Netflix/netflix_aaim08(submitted).pdf.</p>
 *
 * "<p>Collaborative Filtering for Implicit Feedback Datasets" available at
 * http://research.yahoo.com/pub/2433</p>
 *
 * </p>
 * <p>Command line arguments specific to this class are:</p>
 *
 * <ol>
 * <li>--input (path): Directory containing one or more text files with the dataset</li>
 * <li>--output (path): path where output should go</li>
 * <li>--lambda (double): regularization parameter to avoid overfitting</li>
 * <li>--userFeatures (path): path to the user feature matrix</li>
 * <li>--itemFeatures (path): path to the item feature matrix</li>
 * </ol>
 */
public class DistributedParallelALSFactorizationJob extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(DistributedParallelALSFactorizationJob.class);
  private static final String LZO_CODEC_CLASS = "org.apache.hadoop.io.compress.LzoCodec"; 
  private static final int LARGE_MATRIX_MAP_TASKS_NUM = 1000000; 
  private static final String SMALL_MATRIX_MEMORY = "-Xmx2g";
  
  static final String NUM_FEATURES = DistributedParallelALSFactorizationJob.class.getName() + ".numFeatures";
  static final String LAMBDA = DistributedParallelALSFactorizationJob.class.getName() + ".lambda";
  static final String ALPHA = DistributedParallelALSFactorizationJob.class.getName() + ".alpha";
  static final String FEATURE_MATRIX = DistributedParallelALSFactorizationJob.class.getName() + ".featureMatrix";
  static final String FEATURE_MATRIX_TRANSPOSE = DistributedParallelALSFactorizationJob.class.getName() + ".featureMatrixTranspose";
  
  private int numUsers;
  private int numItems;
  
  private boolean implicitFeedback;
  private int numIterations;
  private int numFeatures;
  private double lambda;
  private double alpha;
  private long dfsBlockSize;
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DistributedParallelALSFactorizationJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    addInputOption();
    addOutputOption();
    addOption("lambda", null, "regularization parameter", true);
    addOption("implicitFeedback", null, "data consists of implicit feedback?", String.valueOf(false));
    addOption("alpha", null, "confidence parameter (only used on implicit feedback)", String.valueOf(40));
    addOption("numFeatures", null, "dimension of the feature space", true);
    addOption("numIterations", null, "number of iterations", true);
    addOption("numUsers", null, "number of users", true);
    addOption("numItems", null, "number of items", true);
    addOption("blockSize", null, "dfs block size.", false);
    //addOption("runIterations", null, "true or false for iterations", true);
    
    Map<String,String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }

    numFeatures = Integer.parseInt(parsedArgs.get("--numFeatures"));
    numIterations = Integer.parseInt(parsedArgs.get("--numIterations"));
    lambda = Double.parseDouble(parsedArgs.get("--lambda"));
    alpha = Double.parseDouble(parsedArgs.get("--alpha"));
    implicitFeedback = Boolean.parseBoolean(parsedArgs.get("--implicitFeedback"));
    numUsers = Integer.parseInt(parsedArgs.get("--numUsers"));
    numItems = Integer.parseInt(parsedArgs.get("--numItems"));
    dfsBlockSize = getOption("blockSize") == null ? 64 * 1024 * 1024 : Long.parseLong(getOption("blockSize"));
    /*
        * compute the factorization A = U M'
        *
        * where A (users x items) is the matrix of known ratings
        *           U (users x features) is the representation of users in the feature space
        *           M (items x features) is the representation of items in the feature space
        */

    
    /* create A' */
    Job itemRatings = prepareJob(getInputPath(), pathToItemRatings(),
        TextInputFormat.class, ItemRatingVectorsMapper.class, IntWritable.class,
        VectorWritable.class, VectorSumReducer.class, IntWritable.class,
        VectorWritable.class, SequenceFileOutputFormat.class);
    itemRatings.setCombinerClass(VectorSumReducer.class);
    itemRatings.waitForCompletion(true);
    //numItems = 
    //    (int) itemRatings.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
    log.info("Number of Items\t{}", numItems);
    
    /* create A */
    Job userRatings = prepareJob(pathToItemRatings(), pathToUserRatings(),
        TransposeMapper.class, IntWritable.class, VectorWritable.class, MergeVectorsReducer.class, IntWritable.class,
        VectorWritable.class);
    userRatings.setCombinerClass(MergeVectorsCombiner.class);
    userRatings.waitForCompletion(true);
    //numUsers = 
    //    (int) userRatings.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
    log.info("Number of Users\t{}", numUsers);
    
    /* count item per user */
    Job userItemCntsJob = prepareJob(pathToUserRatings(), getOutputPath("userItemCnts"), SequenceFileInputFormat.class, 
        UserItemCntsMapper.class, IntWritable.class, IntWritable.class,
        UserItemCntsReducer.class, IntWritable.class, IntWritable.class, SequenceFileOutputFormat.class);
    userItemCntsJob.setJobName("user ratings count");
    userItemCntsJob.setCombinerClass(UserItemCntsReducer.class);
    userItemCntsJob.waitForCompletion(true);
    
    
    //TODO this could be fiddled into one of the upper jobs
    Job averageItemRatings = prepareJob(pathToItemRatings(), getTempPath("averageRatings"),
        AverageRatingMapper.class, IntWritable.class, VectorWritable.class, MergeVectorsReducer.class,
        IntWritable.class, VectorWritable.class);
    averageItemRatings.setCombinerClass(MergeVectorsCombiner.class);
    averageItemRatings.waitForCompletion(true);

    Vector averageRatings = ALSMatrixUtil.readFirstRow(getTempPath("averageRatings"), getConf());
    
    /* create an initial M */
    initializeM(averageRatings);

    for (int currentIteration = 0; currentIteration < numIterations; currentIteration++) {
      DistributedRowMatrix curM = 
          new DistributedRowMatrix(pathToM(currentIteration-1), getTempPath("Mtemp" + String.valueOf(currentIteration-1)), 
              numItems, numFeatures);
      curM.setConf(new Configuration());
      DistributedRowMatrix YtransposeY = curM.times(curM);
      
      // broadcast M, read A row-wise, recompute U row-wise //
      log.info("Recomputing U (iteration {}/{})", currentIteration, numIterations);
      runSolver(pathToUserRatings(), pathToU(currentIteration), pathToM(currentIteration - 1), 
          YtransposeY.getRowPath(), numItems);
      
      
      DistributedRowMatrix curU = 
          new DistributedRowMatrix(pathToU(currentIteration), getTempPath("Utmp" + String.valueOf(currentIteration)),
              numUsers, numFeatures);
      curU.setConf(new Configuration());
      DistributedRowMatrix XtransposeX = curU.times(curU);
      
      // set up index of U //
      CreateMapFileFromSeq.createMapFile(pathToU(currentIteration));
      
      // broadcast U, read A' row-wise, recompute M row-wise //
      log.info("Recomputing M (iteration {}/{})", currentIteration, numIterations);
      runDistributedImplicitSolver(pathToItemRatings(), pathToM(currentIteration), 
          pathToU(currentIteration), XtransposeX.getRowPath(), numUsers);
    }
    return 0;
  }
  
  private void initializeM(Vector averageRatings) throws IOException {
    Random random = RandomUtils.getRandom();

    FileSystem fs = FileSystem.get(pathToM(-1).toUri(), getConf());
    SequenceFile.Writer writer = null;
    //MapFile.Writer writer = null;
    try {
      //writer = new MapFile.Writer(getConf(), fs, pathToM(-1).toString(), IntWritable.class, VectorWritable.class);
      writer = new SequenceFile.Writer(fs, getConf(), new Path(pathToM(-1), "part-m-00000"), IntWritable.class,
          VectorWritable.class);

      Iterator<Vector.Element> averages = averageRatings.iterateNonZero();
      while (averages.hasNext()) {
        Vector.Element e = averages.next();
        Vector row = new DenseVector(numFeatures);
        row.setQuick(0, e.get());
        for (int m = 1; m < numFeatures; m++) {
          row.setQuick(m, random.nextDouble());
        }
        writer.append(new IntWritable(e.index()), new VectorWritable(row));
      }
    } finally {
      Closeables.closeQuietly(writer);
    }
  }
  
  private void runSolver(Path ratings, Path output, Path pathToUorI, Path pathToTranspose, int rowNums)
      throws ClassNotFoundException, IOException, InterruptedException {
    
    @SuppressWarnings("rawtypes")
    Class<? extends Mapper> solverMapper = null;
    if (implicitFeedback) {
      solverMapper = SolveImplicitFeedbackMapper.class;
    } else {
      solverMapper = SolveExplicitFeedbackMapper.class;
    }

    Job solverForUorI = prepareJob(ratings, output, SequenceFileInputFormat.class, solverMapper, IntWritable.class,
        VectorWritable.class, SequenceFileOutputFormat.class);
    
    Configuration solverConf = solverForUorI.getConfiguration();
    solverConf.set(LAMBDA, String.valueOf(lambda));
    solverConf.set(ALPHA, String.valueOf(alpha));
    solverConf.setInt(NUM_FEATURES, numFeatures);
    solverConf.set(FEATURE_MATRIX, pathToUorI.toString());
    solverConf.set(FEATURE_MATRIX_TRANSPOSE, pathToTranspose.toString());
    solverConf.setInt("rowNums", rowNums);
    solverConf.set("mapred.child.java.opts", SMALL_MATRIX_MEMORY);
    solverConf.setBoolean("mapred.map.tasks.speculative.execution", false);
    solverConf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
    solverConf.setBoolean("mapred.compress.map.output", true);
    solverConf.set("mapred.map.output.compression.codec", LZO_CODEC_CLASS);
    solverForUorI.waitForCompletion(true);
  }
  
  private void runDistributedImplicitSolver(Path ratings, Path output, Path pathToUorI, Path pathToTranspose, int rowNums) 
      throws IOException, InterruptedException, ClassNotFoundException {
    @SuppressWarnings("rawtypes")
    Class<? extends Mapper> solverMapper = DistributedSolveImplicitFeedbackMapper.class;
    Job solverForUorI = prepareJob(ratings, output, SequenceFileInputFormat.class, solverMapper, IntWritable.class,
        VectorWritable.class, SequenceFileOutputFormat.class);
    
    Configuration solverConf = solverForUorI.getConfiguration();
    
    solverConf.setLong("mapred.min.split.size", dfsBlockSize);
    solverConf.setLong("mapred.max.split.size", dfsBlockSize);
    solverConf.setBoolean("mapred.map.tasks.speculative.execution", false);
    solverConf.setInt("mapred.map.tasks", LARGE_MATRIX_MAP_TASKS_NUM);
    solverConf.setLong("mapred.task.timeout", 600000 * 5);
    solverConf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
    solverConf.set("mapred.child.java.opts", SMALL_MATRIX_MEMORY);
    
    solverConf.set(LAMBDA, String.valueOf(lambda));
    solverConf.set(ALPHA, String.valueOf(alpha));
    solverConf.setInt(NUM_FEATURES, numFeatures);
    solverConf.set(FEATURE_MATRIX, pathToUorI.toString());
    solverConf.set(FEATURE_MATRIX_TRANSPOSE, pathToTranspose.toString());
    solverConf.setInt("rowNums", rowNums);
    solverConf.setBoolean("mapred.compress.map.output", true);
    solverConf.set("mapred.map.output.compression.codec", LZO_CODEC_CLASS);
    solverForUorI.waitForCompletion(true);
  }
  public static class SolveImplicitFeedbackMapper extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable> {
    private ImplicitFeedbackAlternatingLeastSquaresSolver solver;
    
    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      double lambda = Double.parseDouble(ctx.getConfiguration().get(LAMBDA));
      double alpha = Double.parseDouble(ctx.getConfiguration().get(ALPHA));
      int numFeatures = ctx.getConfiguration().getInt(NUM_FEATURES, -1);
      Path YPath = new Path(ctx.getConfiguration().get(FEATURE_MATRIX));
      Path YtransposeYPath = new Path(ctx.getConfiguration().get(FEATURE_MATRIX_TRANSPOSE));
      int rowNums = ctx.getConfiguration().getInt("rowNums", -1);
      //OpenIntObjectHashMap<Vector> Y = ALSMatrixUtil.readMatrixByRows(YPath, ctx.getConfiguration());
      OpenIntObjectHashMap<Vector> Y = ALSMatrixUtil.readMatrixByRows(YPath, ctx);
      Matrix YtransposeY = ALSMatrixUtil.retrieveDistributedRowMatrix(YtransposeYPath, numFeatures, numFeatures);
      //solver = new ImplicitFeedbackAlternatingLeastSquaresSolver(numFeatures, lambda, alpha, Y, YtransposeY);

      Preconditions.checkArgument(numFeatures > 0, "numFeatures was not set correctly!");
    }
    
    @Override
    protected void map(IntWritable userOrItemID, VectorWritable ratingsWritable, Context ctx)
        throws IOException, InterruptedException {
      Vector ratings = new SequentialAccessSparseVector(ratingsWritable.get());

      Vector uiOrmj = solver.solve(ratings);

      ctx.write(userOrItemID, new VectorWritable(uiOrmj));
    }
  }
  
  public static class DistributedSolveImplicitFeedbackMapper extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable> {
    private DistributedImplicitFeedbackAlternatingLeastSquaresSolver solver;
    private MapFile.Reader reader;
    
    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      
      double lambda = Double.parseDouble(ctx.getConfiguration().get(LAMBDA));
      double alpha = Double.parseDouble(ctx.getConfiguration().get(ALPHA));
      int numFeatures = ctx.getConfiguration().getInt(NUM_FEATURES, -1);
      Path YPath = new Path(ctx.getConfiguration().get(FEATURE_MATRIX));
      Path YtransposeYPath = new Path(ctx.getConfiguration().get(FEATURE_MATRIX_TRANSPOSE));
      int rowNums =ctx.getConfiguration().getInt("rowNums", -1);
      
      
      Matrix YtransposeY = fetchDistributedRowMatrix(YtransposeYPath, numFeatures, numFeatures);
      FileSystem fs = FileSystem.get(ctx.getConfiguration());
      reader = new MapFile.Reader(fs, YPath.toString(), ctx.getConfiguration());
      solver = new DistributedImplicitFeedbackAlternatingLeastSquaresSolver(rowNums, numFeatures, 
          lambda, alpha, reader, YtransposeY);

      Preconditions.checkArgument(numFeatures > 0, "numFeatures was not set correctly!");
    }
    
    private Matrix fetchDistributedRowMatrix(Path matrixPath, int numRows, int numCols) {
      Matrix result = new DenseMatrix(numRows, numCols);
      DistributedRowMatrix m = 
          new DistributedRowMatrix(matrixPath, new Path(matrixPath.toString() + "_tmp"), numRows, numCols);
      m.setConf(new Configuration());
      Iterator<MatrixSlice> rows = m.iterator();
      while (rows.hasNext()) {
        MatrixSlice row = rows.next();
        result.assignRow(row.index(), row.vector());
      }
      return result;
    }
    
    @Override
    protected void map(IntWritable userOrItemID, VectorWritable ratingsWritable, Context ctx)
        throws IOException, InterruptedException {
      
      Vector ratings = new SequentialAccessSparseVector(ratingsWritable.get());

      Vector uiOrmj = solver.solve(ratings);

      ctx.write(userOrItemID, new VectorWritable(uiOrmj));
    }
  }
  
  
  public static class SolveExplicitFeedbackMapper extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable> {
    private double lambda;
    private int numFeatures;
    private OpenIntObjectHashMap<Vector> UorM;
    private AlternatingLeastSquaresSolver solver;

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      lambda = Double.parseDouble(ctx.getConfiguration().get(LAMBDA));
      numFeatures = ctx.getConfiguration().getInt(NUM_FEATURES, -1);
      solver = new AlternatingLeastSquaresSolver();
      Path UOrIPath = new Path(ctx.getConfiguration().get(FEATURE_MATRIX));
      //UorM = ALSMatrixUtil.readMatrixByRows(UOrIPath, ctx.getConfiguration());
      UorM = ALSMatrixUtil.readMatrixByRows(UOrIPath, ctx);
      Preconditions.checkArgument(numFeatures > 0, "numFeatures was not set correctly!");
    }

    @Override
    protected void map(IntWritable userOrItemID, VectorWritable ratingsWritable, Context ctx)
        throws IOException, InterruptedException {
      Vector ratings = new SequentialAccessSparseVector(ratingsWritable.get());
      List<Vector> featureVectors = Lists.newArrayList();
      Iterator<Vector.Element> interactions = ratings.iterateNonZero();
      while (interactions.hasNext()) {
        int index = interactions.next().index();
        featureVectors.add(UorM.get(index));
      }
      Vector uiOrmj = solver.solve(featureVectors, ratings, lambda, numFeatures);
      ctx.write(userOrItemID, new VectorWritable(uiOrmj));
    }
  }
  
  public static class AverageRatingMapper extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable> {
    @Override
    protected void map(IntWritable r, VectorWritable v, Context ctx) throws IOException, InterruptedException {
      RunningAverage avg = new FullRunningAverage();
      Iterator<Vector.Element> elements = v.get().iterateNonZero();
      while (elements.hasNext()) {
        avg.addDatum(elements.next().get());
      }
      Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
      vector.setQuick(r.get(), avg.getAverage());
      ctx.write(new IntWritable(0), new VectorWritable(vector));
    }
  }
  
  public static class ItemRatingVectorsMapper extends Mapper<LongWritable,Text,IntWritable,VectorWritable> {
    private static IntWritable outKey = new IntWritable();
    
    @Override
    protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(line.toString());
      int userID = Integer.parseInt(tokens[0]);
      int itemID = Integer.parseInt(tokens[1]);
      float rating = Float.parseFloat(tokens[2]);
    
      Vector ratings = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
      ratings.set(userID, rating);
      outKey.set(itemID);
      ctx.write(outKey, new VectorWritable(ratings));
    }
  }
  
  public static class UserItemCntsMapper extends Mapper<IntWritable, VectorWritable, IntWritable, IntWritable> {
    private static IntWritable result = new IntWritable(1);
    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(key, result);
    }
  }
  
  public static class UserItemCntsReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static IntWritable result = new IntWritable();
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  
  private Path pathToM(int iteration) {
    return iteration == numIterations - 1 ? getOutputPath("M") : getTempPath("M-" + iteration);
  }

  private Path pathToU(int iteration) {
    return iteration == numIterations - 1 ? getOutputPath("U") : getTempPath("U-" + iteration);
  }

  private Path pathToItemRatings() {
    return getTempPath("itemRatings");
  }

  private Path pathToUserRatings() {
    return getOutputPath("userRatings");
  }
  
  
}
