package com.skp.experiment.cf.math.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.mapreduce.MergeVectorsCombiner;
import org.apache.mahout.common.mapreduce.MergeVectorsReducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.skp.experiment.common.mapreduce.AverageVectorMapper;

public class MatrixRowNormalizeJob extends AbstractJob {

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    if (parseArguments(args) == null) {
      return -1;
    }
    Job job = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class, 
        MatrixRowNormalizeMapper.class, IntWritable.class, VectorWritable.class, 
        SequenceFileOutputFormat.class);
    job.setJarByClass(MatrixRowNormalizeJob.class);
    job.setNumReduceTasks(0);
    job.waitForCompletion(true);
    return 0;
  }
  public static Job createMatrixRowNormalizeJob(Path input, Path output, Configuration conf) throws IOException {
    Job job = new Job(conf, "Matrix Row Normalize Job");
    job.setJarByClass(MatrixRowNormalizeJob.class);
    FileSystem fs = FileSystem.get(conf);
    input = fs.makeQualified(input);
    output = fs.makeQualified(output);
    
    FileInputFormat.addInputPath(job,  input);
    FileOutputFormat.setOutputPath(job, output);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    job.setMapperClass(MatrixRowNormalizeMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(VectorWritable.class);
    job.setNumReduceTasks(0);
    return job;
  }
  public static class MatrixRowNormalizeMapper extends
    Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

    @Override
    protected void map(IntWritable key, VectorWritable value, Context context) throws IOException,
    InterruptedException {
      Vector v = value.get();
      Vector norm = v.divide(v.zSum());
      context.write(key, new VectorWritable(norm));
    }
  }
}
