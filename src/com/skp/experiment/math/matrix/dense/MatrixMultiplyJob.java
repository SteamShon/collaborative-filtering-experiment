package com.skp.experiment.math.matrix.dense;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.AbstractJob;

public class MatrixMultiplyJob extends AbstractJob {
  private static String inputPathA;
  private static String inputPathB;
  private static String outputDirPath;
  private static String tempDirPath;
  private static int strategy;
  private static int R1;
  private static int R2;
  private static int I;
  private static int K;
  private static int J;
  private static int IB;
  private static int KB;
  private static int JB;
  
  private static int NIB;
  private static int NKB;
  private static int NJB;
  
  private static boolean useM;
  
  private static int lastIBlockNum;
  private static int lastIBlockSize;
  private static int lastKBlockNum;
  private static int lastKBlockSize;
  private static int lastJBlockNum;
  private static int lastJBlockSize;
  
  private static void initializeConfigure(JobContext context) {
    Configuration conf = context.getConfiguration();
    inputPathA = conf.get("inputPathA");
    inputPathB = conf.get("inputPathB");
    R1 = conf.getInt("R1", 0);
    R2 = conf.getInt("R2", 0);
    I = conf.getInt("I", 0);
    K = conf.getInt("K", 0);
    J = conf.getInt("J", 0);
    IB = conf.getInt("IB", 0);
    KB = conf.getInt("KB", 0);
    JB = conf.getInt("JB", 0);
    NIB = (I-1)/IB + 1;
    NKB = (K-1)/KB + 1;
    NJB = (J-1)/JB + 1;
    lastIBlockNum = NIB - 1;
    lastIBlockSize = I - lastIBlockNum*IB;
    lastKBlockNum = NKB-1;
    lastKBlockSize = K - lastKBlockNum*KB;
    lastJBlockNum = NJB-1;
    lastJBlockSize = J - lastJBlockNum*JB;
  }
  @Override
  public int run(String[] args) throws Exception {
    addOption("inputA", "a", "input path to A matrix");
    addOption("inputB", "b", "input path to B matrix");
    addOutputOption();
    addOption("strategy", null, "strategy", String.valueOf(4));
    addOption("R1", null, "number of reducer for first MR job.");
    addOption("R2", null, "number of reducer for second MR job.");
    addOption("I", null, "A matrix`s row dimension.");
    addOption("K", null, "A matrix`s col dimension.");
    addOption("J", null, "B matrix`s col dimension.");
    addOption("IB", null, "A`s row Block size.");
    addOption("KB", null, "A`s col Block size.");
    addOption("JB", null, "B`s col Block size.");
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    FileSystem fs = FileSystem.get(getConf());
    Path inputPathA = new Path(getOption("inputA"));
    Path inputPathB = new Path(getOption("inputB"));
    
    
    return 0;
  }
  public static class MatrixMultiplyMapper 
    extends Mapper<LongWritable, Text, MatrixKey, MatrixValue> {
    
    private Path path;
    private boolean matrixA;
    private MatrixKey key = new MatrixKey();
    private MatrixValue value = new MatrixValue();
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      initializeConfigure(context);
      FileSplit split = (FileSplit)context.getInputSplit();
      path = split.getPath();
      matrixA = path.toString().startsWith(inputPathA);
    }
    
    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(line.toString());
      int index1 = Integer.parseInt(tokens[0]);
      int index2 = Integer.parseInt(tokens[1]);
      float v = Float.parseFloat(tokens[2]);
      int i = 0;
      int k = 0;
      int j = 0;
      value.setV(v);
      if (matrixA) {
        i = index1;
        k = index2;
        key.setIndex1(i/IB);
        key.setIndex3(k/KB);
        key.setM((byte) 0);
        value.setIndex1(i % IB);
        value.setIndex2(k % KB);
        for (int jb = 0; jb < NJB; jb++) {
          key.setIndex2(jb);
          context.write(key, value);
        }
      } else {
        k = index1;
        j = index2;
        key.setIndex2(j/JB);
        key.setIndex3(k/KB);
        key.setM((byte)1);
        value.setIndex1(k % KB);
        value.setIndex2(j % JB);
        for (int ib = 0; ib < NIB; ib++) {
          key.setIndex1(ib);
          context.write(key, value);
        }
      }
    }
  }
  public static class MatrixMultiplyReducer extends 
    Reducer<MatrixKey, MatrixValue, NullWritable, Text> {
    private float[][] A;
    private float[][] B;
    private float[][] C;
    private int aRowDim, aColDim, bColDim;
    
    public void setup(Context context) {
      initializeConfigure(context);
      A = new float[IB][KB];
      B = new float[KB][JB];
      C = new float[IB][JB];
    }
    public void reduce(MatrixKey key, Iterable<MatrixValue> valueList, Context context) 
      throws IOException, InterruptedException {
      
    }
  }
}
