package com.skp.experiment.common;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import com.skp.experiment.common.mapreduce.KeyValuesCountMapper;
import com.skp.experiment.common.mapreduce.KeyValuesCountReducer;

public class KeyValuesCountJob extends AbstractJob {
  private static final String DELIMETER = ",";
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new KeyValuesCountJob(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("inputFileType", "inType", "inut file format{seq, text}", "text");
    addOption("outputFileType", "outType", "output file format{seq, text}", "text");
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    String inputFileType = getOption("inputFileType");
    String outputFileType = getOption("outputFileType");
    runJob(inputFileType, outputFileType);
    return 0;
  }
  
  @SuppressWarnings("rawtypes")
  private void runJob(String inputFileType, String outputFileType) 
      throws IOException, InterruptedException, ClassNotFoundException {
    Class<? extends InputFormat> inputClass = 
        inputFileType.equals("text") ? TextInputFormat.class : SequenceFileInputFormat.class;
    Class<? extends OutputFormat> outputClass = 
        outputFileType.equals("text") ? TextOutputFormat.class : SequenceFileOutputFormat.class;
    Class<? extends WritableComparable> outKeyClass = 
        outputFileType.equals("text") ? NullWritable.class : Text.class;
    Class<? extends Writable> outValueClass = 
        outputFileType.equals("text") ? Text.class : IntWritable.class;
    String outputType = outputFileType.equals("text") ? "text" : "seq";
    
    Job job = prepareJob(getInputPath(), getOutputPath(), inputClass, 
        KeyValuesCountMapper.class, Text.class, IntWritable.class, 
        KeyValuesCountReducer.class, outKeyClass, outValueClass, 
        outputClass);
    job.getConfiguration().set(KeyValuesCountReducer.DELIMETER_KEY, DELIMETER);
    job.getConfiguration().set(KeyValuesCountReducer.OUTPUT_TYPE, outputType);
    job.waitForCompletion(true);
  }
}
