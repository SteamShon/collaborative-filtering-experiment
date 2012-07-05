package com.skp.experiment.common;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;



public class DistributedRowMatrix2TextJob extends AbstractJob {
  private static final String OUTPUT_FORMAT = "outFormat";
  private static String outputFormat = null;
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DistributedRowMatrix2TextJob(), args);
  }
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(OUTPUT_FORMAT, "of", "output format. {raw, vector}.", "vector");
    
    if (parseArguments(args) == null) {
      return -1;
    }
    
    Path input = getInputPath();
    Path output = getOutputPath();
    outputFormat = getOption(OUTPUT_FORMAT);
    Job job = prepareJob(input, output, 
        SequenceFileInputFormat.class, DistributedRowMatrix2TextMapper.class, 
        NullWritable.class, Text.class, TextOutputFormat.class
        );
    Configuration conf = job.getConfiguration();
    job.setJarByClass(DistributedRowMatrix2TextJob.class);
    conf.set(OUTPUT_FORMAT, outputFormat);
    
    job.waitForCompletion(true);
    return 0;
  }
  public static class DistributedRowMatrix2TextMapper extends 
    Mapper<IntWritable, VectorWritable, NullWritable, Text> {
    private static Text outValue = new Text();
    private static String outputFormat = null;
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      if (conf.get(OUTPUT_FORMAT) != null) {
        outputFormat = conf.get(OUTPUT_FORMAT);
      }
    }

    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
        throws IOException, InterruptedException {
      Iterator<Vector.Element> iter = value.get().iterateNonZero();
      
      if (outputFormat == null || outputFormat.equals("vector")) {
        StringBuffer sb = new StringBuffer();
        sb.append(key.get()).append("\t");
        while (iter.hasNext()) {
          Vector.Element e = iter.next();
          sb.append("\t").append(e.index()).append(":").append(e.get());
        }
        outValue.set(sb.toString());
        context.write(NullWritable.get(), outValue);
      } else if (outputFormat.equals("raw")) {
        while (iter.hasNext()) {
          Vector.Element e = iter.next();
          StringBuffer sb = new StringBuffer();
          sb.append(key.get()).append(",").append(e.index()).append(",").append(e.get());
          outValue.set(sb.toString());
          context.write(NullWritable.get(), outValue);
        }
      }
      /*
      StringBuffer sb = new StringBuffer();
      Iterator<Vector.Element> iter = value.get().iterator();
      sb.append(key.get()).append("\t");
      while (iter.hasNext()) {
        Vector.Element e = iter.next();
        sb.append("\t").append(e.index()).append(":").append(e.get());
      }
      System.out.println(sb.toString());
      outValue.set(sb.toString());
      context.write(NullWritable.get(), outValue);
      */
    }
  }
}
