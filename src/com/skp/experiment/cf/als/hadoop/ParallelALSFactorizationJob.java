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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.impl.common.FullRunningAverage;
import org.apache.mahout.cf.taste.impl.common.RunningAverage;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.mapreduce.MergeVectorsCombiner;
import org.apache.mahout.common.mapreduce.MergeVectorsReducer;
import org.apache.mahout.common.mapreduce.TransposeMapper;
import org.apache.mahout.common.mapreduce.VectorSumReducer;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
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
import com.skp.experiment.cf.evaluate.hadoop.EvaluatorUtil;
import com.skp.experiment.cf.math.hadoop.MatrixDistanceSquaredJob;
import com.skp.experiment.common.HadoopClusterUtil;
import com.skp.experiment.common.Text2DistributedRowMatrixJob;
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
public class ParallelALSFactorizationJob extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(ParallelALSFactorizationJob.class);

  public static final String NUM_FEATURES = ParallelALSFactorizationJob.class.getName() + ".numFeatures";
  public static final String LAMBDA = ParallelALSFactorizationJob.class.getName() + ".lambda";
  public static final String ALPHA = ParallelALSFactorizationJob.class.getName() + ".alpha";
  public static final String FEATURE_MATRIX = ParallelALSFactorizationJob.class.getName() + ".featureMatrix";
  public static final String NUM_ROWS = ParallelALSFactorizationJob.class.getName() + ".numRows";
  public static final String NUM_USERS = ParallelALSFactorizationJob.class.getName() + ".numUsers";
  public static final String NUM_ITEMS = ParallelALSFactorizationJob.class.getName() + ".numItems";
  public static final String FEATURE_MATRIX_TRANSPOSE = ParallelALSFactorizationJob.class.getName() + ".featureMatrixTranspose";
  private static final String DELIMETER = ",";
  
  private boolean implicitFeedback;
  private int numIterations;
  private int numFeatures;
  private double lambda;
  private double alpha;
  private int numTaskTrackers;
  private int numUsers;
  private int numItems;
  private int startIteration;
  private String rmsePerIteration;
  
  private boolean useRMSECurve;
  private boolean cleanUp;
  private boolean useTransform;
  private boolean largeUserFeatures;
  private static long taskTimeout = 600000 * 6;
  private static final int multiplyMapTasks = 100000;
  //private static long minInputSplitSize;
  private static int rateIndex = 2;
  private static final float SAFE_MARGIN = 2.5f;
  
  public ParallelALSFactorizationJob() {};
  public ParallelALSFactorizationJob(Configuration conf) {
    setConf(conf);
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ParallelALSFactorizationJob(), args);
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
    addOption("indexSizes", null, "index sizes Path", true);
    addOption("startIteration", null, "start iteration number", String.valueOf(0));
    addOption("oldM", null, "old M matrix Path.", null);
    addOption("largeUserFeatures", null, "true if user x feature matrix is too large for memory", String.valueOf(true));
    addOption("rmseCurve", null, "true if want to extract rmse curve", String.valueOf(true));
    addOption("cleanUp", null, "true if want to clean up temporary matrix", String.valueOf(true));
    addOption("useTransform", null, "true if using logarithm as transform", String.valueOf(true));
    
    Map<String,String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    
    try {
      Map<String, String> indexSizesTmp = EvaluatorUtil.fetchTextFile(new Path(getOption("indexSizes")), DELIMETER, 
          Arrays.asList(0), Arrays.asList(1));
      
      numFeatures = Integer.parseInt(parsedArgs.get("--numFeatures"));
      numIterations = Integer.parseInt(parsedArgs.get("--numIterations"));
      lambda = Double.parseDouble(parsedArgs.get("--lambda"));
      alpha = Double.parseDouble(parsedArgs.get("--alpha"));
      implicitFeedback = Boolean.parseBoolean(parsedArgs.get("--implicitFeedback"));
      numUsers = Integer.parseInt(indexSizesTmp.get("0"));
      numItems = Integer.parseInt(indexSizesTmp.get("1"));
      
      numTaskTrackers = HadoopClusterUtil.getNumberOfTaskTrackers(getConf()) * multiplyMapTasks;
      startIteration = Integer.parseInt(parsedArgs.get("--startIteration"));
      largeUserFeatures = Boolean.parseBoolean(getOption("largeUserFeatures"));
      useRMSECurve = Boolean.parseBoolean(getOption("rmseCurve"));
      cleanUp = Boolean.parseBoolean(getOption("cleanUp"));
      useTransform = Boolean.parseBoolean(getOption("useTransform"));
      rateIndex = Integer.parseInt(getOption("rateIndex"));
      if (useTransform) {
        // transform price into rating
        Job transformJob = prepareJob(getInputPath(), pathToTransformed(), TextInputFormat.class, 
            TransformColumnValueMapper.class, NullWritable.class, Text.class, 
            TextOutputFormat.class);
        transformJob.waitForCompletion(true);
      } else {
        FileUtil.copy(FileSystem.get(getConf()), getInputPath(), 
            FileSystem.get(getConf()), pathToTransformed(), false, getConf());
      }
      
      if (getOption("oldM") != null) {
        runOnetimeSolver(pathToTransformed(), getOutputPath("U"), new Path(getOption("oldM")));
        return 0;
      }
      /*
          * compute the factorization A = U M'
          *
          * where A (users x items) is the matrix of known ratings
          *           U (users x features) is the representation of users in the feature space
          *           M (items x features) is the representation of items in the feature space
          */
      if (startIteration == 0) {
        // create A' 
        Job itemRatings = prepareJob(pathToTransformed(), pathToItemRatings(),
            TextInputFormat.class, ItemRatingVectorsMapper.class, IntWritable.class,
            VectorWritable.class, VectorSumReducer.class, IntWritable.class,
            VectorWritable.class, SequenceFileOutputFormat.class);
        itemRatings.setCombinerClass(VectorSumReducer.class);
        long matrixSizeExp = (long)(8L * numUsers * numFeatures * SAFE_MARGIN);
        long memoryThreshold = HadoopClusterUtil.PHYSICAL_MEMERY_LIMIT / (long)HadoopClusterUtil.MAP_TASKS_PER_NODE;
        int numTaskPerDataNode = Math.max(1, (int) (HadoopClusterUtil.PHYSICAL_MEMERY_LIMIT / (double)matrixSizeExp));
        //log.info("matrix Size: " + matrixSizeExp + ", memorhThreshold: " + memoryThreshold + ", numTaskPerDataNode: " + numTaskPerDataNode);
        if (matrixSizeExp > memoryThreshold) {
          //log.info("A: {}", numTaskPerDataNode * HadoopClusterUtil.getNumberOfTaskTrackers(getConf()));
          int numReducer = Math.min(numTaskPerDataNode * HadoopClusterUtil.getNumberOfTaskTrackers(getConf()), 
              HadoopClusterUtil.getMaxMapTasks(getConf()));
          //log.info("Number Of Reducer: " + numReducer);
          itemRatings.setNumReduceTasks(numReducer);
        }
        
        itemRatings.waitForCompletion(true);
        
        Job userRatings = prepareJob(pathToItemRatings(), pathToUserRatings(),
            TransposeMapper.class, IntWritable.class, VectorWritable.class, MergeVectorsReducer.class, IntWritable.class,
            VectorWritable.class);
        userRatings.setNumReduceTasks(HadoopClusterUtil.getNumberOfTaskTrackers(getConf()));
        userRatings.setCombinerClass(MergeVectorsCombiner.class);
        userRatings.setNumReduceTasks(HadoopClusterUtil.getMaxMapTasks(getConf()));
        userRatings.waitForCompletion(true);
        
        // count item per user
        Job userItemCntsJob = prepareJob(pathToUserRatings(), getOutputPath("userItemCnt"), SequenceFileInputFormat.class, 
            UserItemCntsMapper.class, IntWritable.class, IntWritable.class, SequenceFileOutputFormat.class);
        userItemCntsJob.setJobName("user ratings count");
        userItemCntsJob.waitForCompletion(true);
        
        //TODO this could be fiddled into one of the upper jobs
        Job averageItemRatings = prepareJob(pathToItemRatings(), getTempPath("averageRatings"),
            AverageRatingMapper.class, IntWritable.class, VectorWritable.class, MergeVectorsReducer.class,
            IntWritable.class, VectorWritable.class);
        averageItemRatings.setCombinerClass(MergeVectorsCombiner.class);
        averageItemRatings.waitForCompletion(true);

        Vector averageRatings = ALSMatrixUtil.readFirstRow(getTempPath("averageRatings"), getConf());


        /** create an initial M */
        initializeM(averageRatings);
      }
      
      for (int currentIteration = startIteration; currentIteration < numIterations; currentIteration++) {
        DistributedRowMatrix curM = 
            new DistributedRowMatrix(pathToM(currentIteration-1), 
                getTempPath("Mtemp/tmp-" + String.valueOf(currentIteration-1) + "/M"), 
                numItems, numFeatures);
        curM.setConf(getConf());
        DistributedRowMatrix YtransposeY = curM.times(curM);
        /** broadcast M, read A row-wise, recompute U row-wise */
        log.info("Recomputing U (iteration {}/{})", currentIteration, numIterations);
        runSolver(pathToUserRatings(), pathToU(currentIteration), pathToM(currentIteration - 1), 
            YtransposeY.getRowPath(), numItems, false);
        
        DistributedRowMatrix curU = 
            new DistributedRowMatrix(pathToU(currentIteration), 
                getTempPath("Utmp/tmp-" + String.valueOf(currentIteration) + "/U"),
                numUsers, numFeatures);
        curU.setConf(getConf());
        DistributedRowMatrix XtransposeX = curU.times(curU);
        
        /** broadcast U, read A' row-wise, recompute M row-wise */
        log.info("Recomputing M (iteration {}/{})", currentIteration, numIterations);
        runSolver(pathToItemRatings(), pathToM(currentIteration), pathToU(currentIteration), 
            XtransposeX.getRowPath(), numUsers, largeUserFeatures);
        
        /** calculate rmse on each updated matrix U, M and decide to further iteration */
        if (currentIteration > startIteration && useRMSECurve) {
          Pair<Integer, Double> UsquaredError = 
              calculateMatrixDistanceSquared(pathToU(currentIteration-1), pathToU(currentIteration), currentIteration);
          Pair<Integer, Double> MsquaredError = 
              calculateMatrixDistanceSquared(pathToM(currentIteration-1), pathToM(currentIteration), currentIteration);
          String currentRMSE = currentIteration + DELIMETER + UsquaredError.getFirst() + 
              DELIMETER + UsquaredError.getSecond() + DELIMETER + MsquaredError.getFirst() + 
              DELIMETER + MsquaredError.getSecond() + EvaluatorUtil.NEWLINE;
          rmsePerIteration += currentRMSE;
          log.info("iteration {}: {}", currentIteration, currentRMSE); 
        }
        if (currentIteration >= startIteration + 2 && cleanUp) {
          FileSystem fs = FileSystem.get(getConf());
          fs.deleteOnExit(pathToU(currentIteration - 2));
          fs.deleteOnExit(pathToM(currentIteration - 2));
        }
      }
      return 0;
    } catch (Exception e) {
      log.info("Exception: {}", e.getMessage());
      return -1;
    } finally {
      if (useRMSECurve) {
        EvaluatorUtil.writeToHdfs(getConf(), getOutputPath("RMSE"), rmsePerIteration);
      }
    }
  }
  private Pair<Integer, Double> calculateMatrixDistanceSquared(Path oldMatrix, Path newMatrix, int iteration) 
      throws IOException, InterruptedException, ClassNotFoundException {
    FileSystem fs = FileSystem.get(getConf());
    Path path = getTempPath("rmse-" + iteration);
    fs.delete(path, true);
    Job rmseJob = MatrixDistanceSquaredJob.createMinusJob(getConf(), oldMatrix, newMatrix, path);
    rmseJob.waitForCompletion(true);
    Pair<Integer, Double> result = MatrixDistanceSquaredJob.retrieveDistanceSquaredOutput(getConf(), path);
    fs.delete(path, true);
    return result;
  }
  
  private void runOnetimeSolver(Path input, Path output, Path oldMPath) throws Exception {
    ToolRunner.run(new Text2DistributedRowMatrixJob(), new String[] {
      "-i", input.toString(), "-o", pathToUserRatings().toString(), 
      "-ri", "0", "-ci", "1", "-vi", "2"
    });
    Path MPath = oldMPath;
    DistributedRowMatrix M = 
        new DistributedRowMatrix(MPath, getTempPath("Mtemp"), numItems, numFeatures);
    M.setConf(new Configuration());
    DistributedRowMatrix YtransposeY = M.times(M);
    
    // recompute U for given input ratings
    Job solverForU = prepareJob(pathToUserRatings(), output, 
        SequenceFileInputFormat.class, 
        ParallelALSFactorizationJob.SolveImplicitFeedbackMapper.class, IntWritable.class, VectorWritable.class,
        SequenceFileOutputFormat.class);
    
    Configuration solverConf = solverForU.getConfiguration();
    solverConf.setBoolean("mapred.map.tasks.speculative.execution", false);
    solverConf.set(ParallelALSFactorizationJob.LAMBDA, String.valueOf(lambda));
    solverConf.set(ParallelALSFactorizationJob.ALPHA, String.valueOf(alpha));
    solverConf.setInt(ParallelALSFactorizationJob.NUM_FEATURES, numFeatures);
    solverConf.set(ParallelALSFactorizationJob.FEATURE_MATRIX, MPath.toString());
    solverConf.set(ParallelALSFactorizationJob.FEATURE_MATRIX_TRANSPOSE, YtransposeY.getRowPath().toString());

    solverConf.setInt("mapred.map.tasks", numTaskTrackers);
    solverConf.setLong("mapred.min.split.size", HadoopClusterUtil.getMinInputSplitSizeMax(getConf(), pathToUserRatings()));
    solverConf.setLong("mapred.max.split.size",  HadoopClusterUtil.getMinInputSplitSizeMax(getConf(), pathToUserRatings()));
    
    solverForU.waitForCompletion(true);
  }
  
  private void initializeM(Vector averageRatings) throws IOException {
    Random random = RandomUtils.getRandom();

    FileSystem fs = FileSystem.get(pathToM(-1).toUri(), getConf());
    SequenceFile.Writer writer = null;
    try {
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
  public static class TransformColumnValueMapper 
  extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static Text outValue = new Text();

    private String buildOutput(String[] tokens) {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < tokens.length; i++) {
        if (i > 0) {
          sb.append(DELIMETER);
        }
        sb.append(tokens[i]);
      }
      return sb.toString();
    }
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(value.toString());
      int sz = tokens.length;
      tokens[sz-1] = String.valueOf(Math.log(Float.parseFloat(tokens[sz-1]) + 1.0f) + 1.0f);
      outValue.set(buildOutput(tokens));
      context.write(NullWritable.get(), outValue);
    }

  }
  public static class ItemRatingVectorsMapper extends Mapper<LongWritable,Text,IntWritable,VectorWritable> {
    private static IntWritable outKey = new IntWritable();
    @Override
    protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(line.toString());
      int sz = tokens.length;
      int userID = Integer.parseInt(tokens[0]);
      int itemID = Integer.parseInt(tokens[1]);
      float rating = Float.parseFloat(tokens[sz-1]);
      
      
      Vector ratings = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
      ratings.set(userID, rating);
      outKey.set(itemID);
      ctx.write(outKey, new VectorWritable(ratings));
    }
  }
  
  private void runSolver(Path ratings, Path output, Path pathToUorI, Path pathToTranspose, int numRows, boolean largeMatrix)
      throws ClassNotFoundException, IOException, InterruptedException {
    @SuppressWarnings("rawtypes")
    Class<? extends Mapper> solverMapper = implicitFeedback ?
        SolveImplicitFeedbackMapper.class : SolveExplicitFeedbackMapper.class;

    Job solverForUorI = prepareJob(ratings, output, SequenceFileInputFormat.class, solverMapper, IntWritable.class,
        VectorWritable.class, SequenceFileOutputFormat.class);
    
    Configuration solverConf = solverForUorI.getConfiguration();
    
    long matrixSizeExp = (long)(8L * numRows * numFeatures * SAFE_MARGIN);
    long memoryThreshold = HadoopClusterUtil.PHYSICAL_MEMERY_LIMIT / HadoopClusterUtil.MAP_TASKS_PER_NODE;
    int numTaskPerDataNode = Math.max(1, (int) (HadoopClusterUtil.PHYSICAL_MEMERY_LIMIT / matrixSizeExp));
    
    if (matrixSizeExp > memoryThreshold) {
      solverConf.set("mapred.child.java.opts", "-Xmx8g");
      solverConf.set("mapred.map.child.java.opts", "-Xmx8g");
      solverConf.setLong("dfs.block.size", HadoopClusterUtil.getMaxBlockSize(getConf(), pathToTransformed()));
      solverConf.setInt("mapred.map.tasks", HadoopClusterUtil.getNumberOfTaskTrackers(getConf()));
      solverConf.setLong("mapred.min.split.size", HadoopClusterUtil.getMaxBlockSize(getConf(), pathToTransformed()));
      solverConf.setLong("mapred.max.split.size", HadoopClusterUtil.getMaxBlockSize(getConf(), pathToTransformed()));
      solverConf.set("lock.file", pathToHostLocks().toString());
      solverConf.setInt("lock.file.nums", Math.min(HadoopClusterUtil.MAP_TASKS_PER_NODE, numTaskPerDataNode));
    } else {
      solverConf.setLong("mapred.min.split.size", HadoopClusterUtil.getMinInputSplitSizeMax(getConf(), ratings));
      solverConf.setLong("mapred.max.split.size", HadoopClusterUtil.getMinInputSplitSizeMax(getConf(), ratings));
      solverConf.setInt("mapred.map.tasks", HadoopClusterUtil.getNumberOfTaskTrackers(getConf()) * multiplyMapTasks);
      //solverConf.setBoolean("mapred.map.tasks.speculative.execution", false);
    }
    solverConf.setLong("mapred.task.timeout", taskTimeout);
    solverConf.setBoolean("mapred.map.tasks.speculative.execution", false);
    
    solverConf.set(LAMBDA, String.valueOf(lambda));
    solverConf.set(ALPHA, String.valueOf(alpha));
    solverConf.setInt(NUM_FEATURES, numFeatures);
    solverConf.setInt(NUM_ROWS, numRows);
    solverConf.set(FEATURE_MATRIX, pathToUorI.toString());
    solverConf.set(FEATURE_MATRIX_TRANSPOSE, pathToTranspose.toString());
    
    solverForUorI.waitForCompletion(true);
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
  
  public static class SolveImplicitFeedbackMapper extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable> {

    private ImplicitFeedbackAlternatingLeastSquaresSolver solver;
    private String lockPath = null;
    private long sleepPeriod = 30000;
    private int lockNums;
    private Path currentLockPath = null;
    
    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      Configuration conf = ctx.getConfiguration();
      double lambda = Double.parseDouble(ctx.getConfiguration().get(LAMBDA));
      double alpha = Double.parseDouble(ctx.getConfiguration().get(ALPHA));
      int numFeatures = ctx.getConfiguration().getInt(NUM_FEATURES, -1);
      int numRows = ctx.getConfiguration().getInt(NUM_ROWS, -1);
      Path YPath = new Path(ctx.getConfiguration().get(FEATURE_MATRIX));
      Path YtransposeYPath = new Path(ctx.getConfiguration().get(FEATURE_MATRIX_TRANSPOSE));
      lockPath = conf.get("lock.file"); 
      lockNums = conf.getInt("lock.file.nums", 1);
      if (lockPath != null) {
        checkLock(ctx, lockNums);
      }
      
      //OpenIntObjectHashMap<Vector> Y = ALSMatrixUtil.readMatrixByRows(YPath, ctx.getConfiguration());
      //OpenIntObjectHashMap<Vector> Y = ALSMatrixUtil.readMatrixByRows(YPath, ctx);
      Matrix Y = ALSMatrixUtil.readMatrixByRows(YPath, ctx, numRows, numFeatures);
      Matrix YtransposeY = ALSMatrixUtil.retrieveDistributedRowMatrix(YtransposeYPath, numFeatures, numFeatures);
      
      solver = new ImplicitFeedbackAlternatingLeastSquaresSolver(numFeatures, lambda, alpha, Y, YtransposeY);

      Preconditions.checkArgument(numFeatures > 0, "numFeatures was not set correctly!");
    }
    private void checkLock(Context ctx, int lockNums) throws InterruptedException, IOException {
      InetAddress thisIp =InetAddress.getLocalHost();
      String hostIp = thisIp.getHostAddress();
      
      // busy wait
      Configuration conf = ctx.getConfiguration();
      long totalSleep = 0;
      boolean haveLock = false;
      FileSystem fs = FileSystem.get(conf);
      while (haveLock == false) {
        for (int i = 0; i < lockNums; i++) {
          Path checkPath = new Path(lockPath, hostIp + "_" + i);
          if (fs.exists(checkPath) == false) {
            haveLock = true;
            currentLockPath = checkPath;
            BufferedWriter br = new BufferedWriter(
                new OutputStreamWriter(fs.create(currentLockPath)));
            br.write(ctx.getTaskAttemptID().toString());
            break;
          }
        }
        if (haveLock == false) {
          Random random = new Random();
          int diff = 1000 + random.nextInt(1000) % 1000;
          totalSleep += diff + sleepPeriod;
          ctx.setStatus("sleeping: " + String.valueOf(totalSleep));
          Thread.sleep(sleepPeriod + diff);
        } 
      }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      if (currentLockPath != null) {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        fs.deleteOnExit(currentLockPath);
      }
    }

    @Override
    protected void map(IntWritable userOrItemID, VectorWritable ratingsWritable, Context ctx)
        throws IOException, InterruptedException {
      Vector ratings = new SequentialAccessSparseVector(ratingsWritable.get());

      Vector uiOrmj = solver.solve(ratings);
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
  
  public static class UserItemCntsMapper extends Mapper<IntWritable, VectorWritable, IntWritable, IntWritable> {
    private static IntWritable result = new IntWritable(1);
    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
        throws IOException, InterruptedException {
      result.set(value.get().getNumNondefaultElements());
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
  
  private Path pathToHostLocks() {
    return getTempPath("hosts");
  }
  
  private Path pathToTransformed() {
    return getTempPath("transfomed");
  }
}
