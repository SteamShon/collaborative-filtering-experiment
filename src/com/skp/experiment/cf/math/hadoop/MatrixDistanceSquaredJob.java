package com.skp.experiment.cf.math.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.io.Closeables;
import com.skp.experiment.common.mapreduce.IdentityMapper;

public class MatrixDistanceSquaredJob extends AbstractJob {
  private static final int RESULT_COLUMN_INDEX = 0;
  private MatrixDistanceSquaredJob() {}
  
  public static Job createMinusJob(Configuration conf, Path inputA, 
      Path inputB, Path output) throws IOException {
    
    Job job = new Job(conf);
    job.setJarByClass(MatrixDistanceSquaredJob.class);
    FileSystem fs = FileSystem.get(conf);
    inputA = fs.makeQualified(inputA);
    inputB = fs.makeQualified(inputB);
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    FileInputFormat.addInputPath(job, inputA);
    FileInputFormat.addInputPath(job, inputB);
    FileOutputFormat.setOutputPath(job, output);
    
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(MatrixDistanceSquaredReducer.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(VectorWritable.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    
    return job;
  }
  
  public static Vector retrieveDistanceSquaredOutputVector(Configuration conf, Path outputPath) 
      throws IOException {
    Vector rmseVector = new RandomAccessSparseVector(Integer.MAX_VALUE);
    SequenceFileDirIterator<IntWritable, VectorWritable> iter = 
        new SequenceFileDirIterator<IntWritable, VectorWritable>(outputPath, 
            PathType.GLOB, null, null, true, conf);
    try {
      while (iter.hasNext()) {
        Pair<IntWritable, VectorWritable> cur = iter.next();
        rmseVector.set(cur.getFirst().get(), cur.getSecond().get().get(RESULT_COLUMN_INDEX));
      }
    } finally {
      Closeables.closeQuietly(iter);
    }
    return rmseVector;
  }
  
  public static Pair<Integer, Double> retrieveDistanceSquaredOutput(Configuration conf, Path outputPath) 
      throws IOException {
    Double squaredError = 0.0;
    Integer count = 0;
    SequenceFileDirIterator<IntWritable, VectorWritable> iter = 
        new SequenceFileDirIterator<IntWritable, VectorWritable>(new Path(outputPath, "part*"), 
            PathType.GLOB, null, null, true, conf);
    try {
      while (iter.hasNext()) {
        count++;
        Pair<IntWritable, VectorWritable> cur = iter.next();
        squaredError += cur.getSecond().get().get(RESULT_COLUMN_INDEX);
      }
    } finally {
      Closeables.closeQuietly(iter);
    }
    return new Pair<Integer, Double>(count, squaredError);
  }
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    Job job = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class,
        IdentityMapper.class, IntWritable.class, VectorWritable.class, 
        MatrixDistanceSquaredReducer.class, IntWritable.class, VectorWritable.class, 
        SequenceFileOutputFormat.class);
    job.waitForCompletion(true);
    return 0;
  }
  
  
  public static class MatrixDistanceSquaredReducer 
    extends Reducer<Writable, VectorWritable, Writable, VectorWritable> {
    private static Vector outVector = new RandomAccessSparseVector(1);
    @Override
    protected void reduce(Writable key, Iterable<VectorWritable> values, 
        Context context) throws IOException, InterruptedException {
      Iterator<VectorWritable> iter = values.iterator();
      Vector sourceVector = iter.next().get();
      Vector targetVector = iter.next().get();
      outVector.set(RESULT_COLUMN_INDEX, sourceVector.getDistanceSquared(targetVector));
      context.write(key, new VectorWritable(outVector));
    }
  }
  
}
