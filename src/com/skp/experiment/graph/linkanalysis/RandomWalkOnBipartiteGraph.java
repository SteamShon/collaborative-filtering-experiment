package com.skp.experiment.graph.linkanalysis;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.apache.mahout.math.CardinalityException;
import org.apache.mahout.math.DistributedRowMatrixWriter;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.TransposeJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closeables;
import com.skp.experiment.cf.evaluate.hadoop.EvaluatorUtil;
import com.skp.experiment.cf.math.hadoop.MatrixMultiplyWithThresholdJob;
import com.skp.experiment.cf.math.hadoop.MatrixRowNormalizeJob;
import com.skp.experiment.common.DistributedRowMatrix2TextJob;
import com.skp.experiment.common.HadoopClusterUtil;
import com.skp.experiment.common.OptionParseUtil;
import com.skp.experiment.common.Text2DistributedRowMatrixJob;
import com.skp.experiment.common.join.ImprovedRepartitionJoinAndFilterJob;
import com.skp.experiment.common.mapreduce.TopKVectorMapper;

/*
 * this implement A random walk algoriothm in http://www2008.org/papers/pdf/p61-fuxmanA.pdf.
 * assumes that input graph is bipartite 
 * Bipartite graph consist of 
 * Left vertices L: users
 * Right vertices R: items
 * Edge between L-R: confidence between <u, i>. ex) rating for given item by given user
 */
public class RandomWalkOnBipartiteGraph extends AbstractJob {
  private static int numItems;
  private static int numUsers;
  private static int iterations;
  private static int topK;
  private static float gamma;
  private static int startIteration;
  
  private static final String NUM_ITEMS = "numCols";
  private static final String NUM_USERS = "numRows";
  private static final String ITERATIONS = "iterations";
  private static final String TOP_K = "topK";
  
  private static final Logger log = LoggerFactory.getLogger(RandomWalkOnBipartiteGraph.class);
 
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RandomWalkOnBipartiteGraph(), args);
  }
  
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("indexSizes", null, "path to index sizes");
    
    addOption(ITERATIONS, "iteration", "number of iterations.");
    addOption("gamma", null, "gamma", String.valueOf(0.00000001));
    addOption("topK", null, "topK", String.valueOf(100));
    addOption("startIteration", null, "start iterations", String.valueOf(0));
    
    
    if (parseArguments(args) == null) {
      return -1;
    }
    Map<String, String> indexSizesTmp = 
        EvaluatorUtil.fetchTextFile(new Path(getOption("indexSizes")), OptionParseUtil.DELIMETER, 
        Arrays.asList(0), Arrays.asList(1));
    numUsers = Integer.parseInt(indexSizesTmp.get("0"));
    numItems = Integer.parseInt(indexSizesTmp.get("1"));
    
    iterations = Integer.parseInt(getOption(ITERATIONS));
    gamma = Float.parseFloat(getOption("gamma"));
    topK = Integer.parseInt(getOption("topK"));
    startIteration = Integer.parseInt(getOption("startIteration"));
    
    // 1. convert csv format into distributed row matrix form
    log.info("convert csv into distributed row matrix");
    ToolRunner.run(getConf(), new Text2DistributedRowMatrixJob(), new String[] {
      "--input", getInputPath().toString(), "--output", pathToInitialEdges().toString(), 
      "--rowidx", "0", "--colidx", "1", "--valueidx", "2", "--numCols", Integer.toString(numItems)
    });
    ToolRunner.run(getConf(), new Text2DistributedRowMatrixJob(), new String[] {
      "--input", getInputPath().toString(), "--output", pathToInitialEdgesTranspose().toString(),
      "--rowidx", "1", "--colidx", "0", "--valueidx", "2", "--numCols", Integer.toString(numUsers)
    });

    log.info("build normalized edges.");
    // 2. normalize each matrix by row
    ToolRunner.run(getConf(), new MatrixRowNormalizeJob(), new String[]{
      "--input", pathToInitialEdges().toString(), "--output", pathToRowNormEdges().toString()
    });
    ToolRunner.run(getConf(), new MatrixRowNormalizeJob(), new String[] {
      "--input", pathToInitialEdgesTranspose().toString(), "--output", pathToRowNormEdgesTranspose().toString()
    });
    
    
    
    DistributedRowMatrix UIRowNorm = 
        new DistributedRowMatrix(pathToRowNormEdges(), getTempPath("tmp"), numUsers, numItems);
    UIRowNorm.setConf(getConf());
    DistributedRowMatrix IUNorm = UIRowNorm.transpose();
    
    DistributedRowMatrix IURowNorm = 
        new DistributedRowMatrix(pathToRowNormEdgesTranspose(), getTempPath("tmp"), numItems, numUsers);
    IURowNorm.setConf(getConf());
    DistributedRowMatrix UINorm = IURowNorm.transpose();
    
    log.info("before for loop");
    
    //System.out.println("print UINorm");
    //printDistributedRowMatrix(UINorm, getTempPath("UINormConv"));

    //System.out.println("print IUNorm");
    //printDistributedRowMatrix(IUNorm, getTempPath("IUNormConv"));

    log.info("initialize class");
    DistributedRowMatrix initialClass = createDigonalClass(numItems, getTempPath("initial.class"), getConf());
    //printDistributedRowMatrix(initialClass, getTempPath("initial.class.conv"));
    
    DistributedRowMatrix CItranspose = initialClass.transpose();
    
    //DistributedRowMatrix CUtranspose = IUNorm.transpose();
    DistributedRowMatrix CUtranspose = null;
    
    for (int iteration = startIteration; iteration < iterations; iteration++) {
      log.info("current iteration: {}", iteration);
      
      //System.out.println("CU: " + CUtranspose.numCols() + "\t" + CUtranspose.numRows());
      //System.out.println("CI: " + CItranspose.numCols() + "\t" + CItranspose.numRows());
      CUtranspose = timesWithThreshold(CItranspose, IUNorm, pathToProbsCU(iteration), gamma).transpose();
      //printDistributedRowMatrix(CUtranspose, getTempPath("CU.transpose.conv." + Integer.toString(iteration)));
      //System.out.println("CU: " + CUtranspose.numCols() + "\t" + CUtranspose.numRows());
      CItranspose = timesWithThreshold(CUtranspose, UINorm, pathToProbsCI(iteration), gamma);
      //printDistributedRowMatrix(CItranspose, getTempPath("CI.transpose.conv." + Integer.toString(iteration)));
    }
    //printDistributedRowMatrix(CUtranspose, getOutputPath());
    if (retrieveTopKPerUser(CUtranspose.getRowPath(), getTempPath("output")) != 0) {
      return -1;
    }
    if (appendIsDirectFlag(getInputPath(), getTempPath("output"), getOutputPath()) != 0) {
      return -1;
    }
    return 0;
  }
  
  @SuppressWarnings("deprecation")
  public DistributedRowMatrix timesWithThreshold(DistributedRowMatrix src, DistributedRowMatrix other, Path output, float threshold) 
      throws IOException, InterruptedException, ClassNotFoundException {
    if (src.numRows() != other.numRows()) {
      throw new CardinalityException(src.numRows(), other.numRows());
    }
    // multiply
    Configuration initialConf = getConf();
    initialConf.set("mapred.child.java.opts", "-Xmx4g");
    initialConf.setLong("mapred.task.timeout", 600000 * 10);
    
    @SuppressWarnings("deprecation")
    JobConf conf =
        MatrixMultiplyWithThresholdJob.createMatrixMultiplyWithThresholdJob(initialConf, src.getRowPath(), other.getRowPath(),
            output, other.numCols(), threshold);
    JobClient.runJob(conf);
    // prune
    /*
    log.info("prunning with threshold {}", threshold);
    Job pruneJob = PruneVectorWithThreasholdJob.createPruneVectorWithThresholdJob(new Configuration(), getTempPath("multiply"), output);
    pruneJob.getConfiguration().setFloat(PruneVectorWithThreasholdJob.THRESHOLD, threshold);
    pruneJob.waitForCompletion(true);
    // return distributedRowMatrix form
    */
    DistributedRowMatrix out = new DistributedRowMatrix(output, getTempPath("tmp"), src.numCols(), other.numCols());
    out.setConf(conf);
    return out;
  }
  
  private void printDistributedRowMatrix(DistributedRowMatrix matrix, Path output) throws Exception {
    ToolRunner.run(new DistributedRowMatrix2TextJob(), new String[] {
      "--input", matrix.getRowPath().toString(), "--output", 
      output.toString()
    });
  }
  
  /* */
  private int indexVertices(Path verticesPath, Path indexPath) throws IOException {
    FileSystem fs = FileSystem.get(verticesPath.toUri(), getConf());
    SequenceFile.Writer writer = null;
    int index = 0;

    try {
      writer = SequenceFile.createWriter(fs, getConf(), indexPath, IntWritable.class, IntWritable.class);

      for (FileStatus fileStatus : fs.listStatus(verticesPath)) {
        InputStream in = null;
        try {
          in = HadoopUtil.openStream(fileStatus.getPath(), getConf());
          for (String line : new FileLineIterable(in)) {
            writer.append(new IntWritable(index++), new IntWritable(Integer.parseInt(line)));
          }
        } finally {
          Closeables.closeQuietly(in);
        }
      }
    } finally {
      Closeables.closeQuietly(writer);
    }
    return index;
  }
  
  
  //TODO
  private DistributedRowMatrix retrieveClassMatrix(Path classMatrixPath) {
    return null;
  }
  
  
  /* this method returns diagonal matrix with numItems x numItems cardinality */
  private DistributedRowMatrix createDigonalClass(int numItems, Path digonalMatrixPath, Configuration conf) throws IOException {
  //private Matrix createDigonalClass(int numItems) throws IOException {
    Matrix digonalMatrix = new SparseMatrix(numItems, numItems);
    for (int idx = 0; idx < numItems; idx++) {
      digonalMatrix.set(idx, idx, 1.0);
    }
    /*
    Iterator<MatrixSlice> iter = digonalMatrix.iterator();
    while (iter.hasNext()) {
      MatrixSlice row = iter.next();
      Iterator<Vector.Element> cols = row.vector().iterator();
      while (cols.hasNext()) {
        Vector.Element e = cols.next();
        System.out.println(row.index() + "\t" + e.index() + ":" + e.get());
      }
    }
    */
    DistributedRowMatrixWriter.write(digonalMatrixPath, conf, digonalMatrix);
    DistributedRowMatrix result = new DistributedRowMatrix(digonalMatrixPath, 
                                                           getTempPath("digonalClass"), 
                                                           numItems, 
                                                           numItems);
    result.setConf(conf);
    return result;
  }
  
  private int retrieveTopKPerUser(Path input, Path output) 
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = prepareJob(input, output, SequenceFileInputFormat.class, 
        TopKVectorMapper.class, NullWritable.class, Text.class, 
        TextOutputFormat.class);
    job.setJarByClass(RandomWalkOnBipartiteGraph.class);
    job.setJobName("Retrieve Top K Per User.");
    job.getConfiguration().setInt(TopKVectorMapper.TOP_K, topK);
    job.setNumReduceTasks(0);
    if (!job.waitForCompletion(true)) {
      return -1;
    }
    return 0;
  }
  
  private int appendIsDirectFlag(Path train, Path tmpOutput, Path output) throws Exception {
    ToolRunner.run(new ImprovedRepartitionJoinAndFilterJob(), new String[]{
      "-i", tmpOutput.toString(), "-o", output.toString(), 
      "-sidx", "0,1", "-tgt", train.toString() + ":0,1:0,1:2:outer"
    });
    return 0;
  }
  
  public Path pathToInitialEdges() {
    return getTempPath("initial.edges");
  }
  public Path pathToInitialEdgesTranspose() {
    return getTempPath("initial.edges.transpose");
  }
  public Path pathToRowNormEdges() {
    return getTempPath("row.norm");
  }
  public Path pathToRowNormEdgesTranspose() {
    return getTempPath("row.norm.transpose");
  }
  public Path pathToProbsCU(int n) {
    return getTempPath("prob.CU." + Integer.toString(n));
  }
  public Path pathToProbsCI(int n) {
    return getTempPath("prob.CI." + Integer.toString(n));
  }
  public Path pathToUINormConv() {
    return getTempPath("matrix.UINorm");
  }
  public Path pathToIUNormConv() {
    return getTempPath("matrix.IUNorm");
  }
}
