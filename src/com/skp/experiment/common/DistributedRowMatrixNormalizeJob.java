package com.skp.experiment.common;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
/**
 */
public class DistributedRowMatrixNormalizeJob extends AbstractJob {
  private static final String NORM_OUTPUT_TYPE = DistributedRowMatrixNormalizeJob.class.getName() + ".normOutputType";
  private static final String REVERSE_OPTION = DistributedRowMatrixNormalizeJob.class.getName() + ".reverseOption";
  
  private static final String DELIMETER = ",";
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DistributedRowMatrixNormalizeJob(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("outputType", "otype", "output type{true if vector, otherwise false}", String.valueOf(true));
    addOption("reverse", "r", "true if want to reverse row idx and col idx(transpose).", String.valueOf(false));
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    
    @SuppressWarnings("rawtypes")
    Class<? extends WritableComparable> keyClass = 
        getOption("outputType").equals("true") ? IntWritable.class : NullWritable.class;
    Class<? extends Writable> valueClass = 
        getOption("outputType").equals("true") ? VectorWritable.class : Text.class;
    @SuppressWarnings("rawtypes")
    Class<? extends OutputFormat> outFileFormat = 
        getOption("outputType").equals("true") ? SequenceFileOutputFormat.class : TextOutputFormat.class;
    
    Job normJob = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class, 
        VectorNormMapper.class, keyClass, valueClass, 
        outFileFormat);
    normJob.getConfiguration().setBoolean(NORM_OUTPUT_TYPE, getOption("outputType").equals("true") ? true : false);
    normJob.getConfiguration().setBoolean(REVERSE_OPTION, getOption("reverse").equals("true") ? true : false);
    normJob.waitForCompletion(true);
    return 0;
  }
  @SuppressWarnings("rawtypes")
  public static class VectorNormMapper extends 
    Mapper<IntWritable,VectorWritable,WritableComparable,Writable> {
    private static Text outValue = new Text();
    private static boolean vectorOutput = true;
    private static boolean reverse = false;
    @Override
    protected void setup(Context ctx) throws IOException,
        InterruptedException {
      vectorOutput = ctx.getConfiguration().getBoolean(NORM_OUTPUT_TYPE, true);
      reverse = ctx.getConfiguration().getBoolean(REVERSE_OPTION, false);
    }

    @Override
    protected void map(IntWritable row, VectorWritable vectorWritable, Context ctx)
        throws IOException, InterruptedException {
      
      Vector normVector = vectorWritable.get().normalize();
      if (vectorOutput) {
        ctx.write(row, new VectorWritable(normVector));
      } else {
        Iterator<Vector.Element> iter = normVector.iterateNonZero();
        while (iter.hasNext()) {
          Vector.Element e = iter.next();
          if (reverse) {
            outValue.set(e.index() + DELIMETER + row.get() + DELIMETER + e.get());
          } else {
            outValue.set(row.get() + DELIMETER + e.index() + DELIMETER + e.get());
          }
          ctx.write(NullWritable.get(), outValue);
        }
      }
    }
  }
}
