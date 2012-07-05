package com.skp.experiment.cf.math.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class PruneVectorWithThreasholdJob extends AbstractJob {
  public static final String THRESHOLD = PruneVectorWithThreasholdJob.class.getName() + ".threshold";
  
  public static Job createPruneVectorWithThresholdJob(Configuration initConf, Path input, Path output) throws IOException {
    Job job = new Job(initConf, PruneVectorWithThreasholdJob.class.getName());
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapperClass(PruneVectorWithThresholdMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(VectorWritable.class);
    job.setJarByClass(PruneVectorWithThreasholdJob.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    job.setNumReduceTasks(0);
    return job;
  }
  
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    Map<String, String> parsedArg = parseArguments(args);
    if (parsedArg == null) {
      return -1;
    }
    Job job = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class, 
        PruneVectorWithThresholdMapper.class, IntWritable.class, VectorWritable.class, 
        SequenceFileOutputFormat.class);
    job.waitForCompletion(true);
    return 0;
  }
  
  public static class PruneVectorWithThresholdMapper extends 
    Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
    private static float threshold;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      threshold = context.getConfiguration().getFloat(THRESHOLD, 0);
    }

    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
        throws IOException, InterruptedException {
      Vector colVectors = value.get();
      Iterator<Vector.Element> cols = colVectors.iterateNonZero();
      Vector result = new RandomAccessSparseVector(colVectors.size(), colVectors.getNumNondefaultElements());
      while (cols.hasNext()) {
        Vector.Element col = cols.next();
        if (col.get() < threshold) {
          continue;
        }
        result.set(col.index(), col.get());
      }
      context.write(key, new VectorWritable(result));
    }
    
  }
}
