package com.skp.experiment.cf.math.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterator;
import org.apache.mahout.common.mapreduce.MergeVectorsCombiner;
import org.apache.mahout.common.mapreduce.MergeVectorsReducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.io.Closeables;
import com.skp.experiment.common.mapreduce.AverageVectorMapper;
/*
 * read DistributedRowMatrix(<WritableComparable, VectorWritable> as input
 * and aggregate row and write <NullWritable, VectorWritable>
 */
public class MatrixRowAggregateJob extends AbstractJob {
 
  private MatrixRowAggregateJob() {}
  
  public static Job createAggregateRowJob( 
      Path matrixInputPath,
      Path outputVectorPathBase) throws IOException {
    Job job = new Job(new Configuration(), "DistributedRowMatrix aggregate row job");
    job.setJarByClass(MatrixRowAggregateJob.class);
    Configuration conf = job.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    matrixInputPath = fs.makeQualified(matrixInputPath);
    outputVectorPathBase = fs.makeQualified(outputVectorPathBase);
    
    FileInputFormat.addInputPath(job,  matrixInputPath);
    FileOutputFormat.setOutputPath(job, outputVectorPathBase);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    job.setMapperClass(AverageVectorMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(VectorWritable.class);
    job.setReducerClass(MergeVectorsReducer.class);
    job.setCombinerClass(MergeVectorsCombiner.class);
    
    return job;
  }
  
  public static Vector retrieveAggregatedRowOutputVector(Job job) throws IOException {
    Path outputPath = FileOutputFormat.getOutputPath(job);
    Path outputFile = new Path(outputPath, "part-00000");
    SequenceFileValueIterator<VectorWritable> iterator = 
        new SequenceFileValueIterator<VectorWritable>(outputFile, true, job.getConfiguration());
    try {
      return iterator.next().get();
    } finally {
      Closeables.closeQuietly(iterator);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    if (parseArguments(args) == null) {
      return -1;
    }
    Job job = prepareJob(getInputPath(), getOutputPath(), 
        SequenceFileInputFormat.class, AverageVectorMapper.class, 
        NullWritable.class, VectorWritable.class, 
        MergeVectorsReducer.class, NullWritable.class, VectorWritable.class, SequenceFileOutputFormat.class);
    job.setJarByClass(MatrixRowAggregateJob.class);
    job.setCombinerClass(MergeVectorsCombiner.class);
    job.waitForCompletion(true);
    return 0;
  }
}
