package com.skp.experiment.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.AbstractJob;

import com.skp.experiment.cf.evaluate.hadoop.EvaluatorUtil;

public class DistinctColumnValuesJob extends AbstractJob {
  public static final String COLUMN_INDEXS = DistinctColumnValuesJob.class.getName() + ".columnIndexs";
  public static final String DELIMETER = ",";
  private static final String NEWLINE = System.getProperty("line.separator");
  public static enum COUNT {
    TOTAL_COUNT
  };
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("columnIndexs", "cidx", "column indexs for distinct.");
    
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    Job distinctJob = prepareJob(getInputPath(), getOutputPath(), TextInputFormat.class,
        DistinctColumnValuesMapper.class, Text.class, NullWritable.class, 
        DistinctColumnValuesReducer.class, NullWritable.class, Text.class,
        TextOutputFormat.class);
    distinctJob.getConfiguration().set(COLUMN_INDEXS, getOption("columnIndexs"));
    distinctJob.waitForCompletion(true);
    
    EvaluatorUtil.deletePartFiles(getConf(), getOutputPath());
    return 0;
  }
  
  public static class DistinctColumnValuesMapper extends 
    Mapper<LongWritable, Text, Text, NullWritable> {
    private static List<Integer> columnIndexs;
    private static Text outKey = new Text();
    private static IntWritable outValue = new IntWritable();
   
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      columnIndexs = OptionParseUtil.decode(
          context.getConfiguration().get(COLUMN_INDEXS), DELIMETER);
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(value.toString());
      
      for (Integer idx : columnIndexs) {
        if (idx >= 0 && idx < tokens.length) {
          outKey.set(tokens[idx] + DELIMETER + idx);
          outValue.set(idx);
          context.write(outKey, NullWritable.get());
        }
      }
    }
  }
  public static class DistinctColumnValuesReducer extends 
    Reducer<Text, NullWritable, NullWritable, Text> {
    private static List<Integer> columnIndexs;
    private static Map<Integer, FSDataOutputStream> outStreams;
    @Override
    protected void setup(Context context) 
        throws IOException, InterruptedException {
      outStreams = new HashMap<Integer, FSDataOutputStream>();
      FileSystem fs = FileSystem.get(context.getConfiguration());
      columnIndexs = OptionParseUtil.decode(context.getConfiguration().get(COLUMN_INDEXS), DELIMETER);
      Path outputPath = FileOutputFormat.getOutputPath(context);
      String taskId = OptionParseUtil.getAttemptId(context.getConfiguration());
      for (Integer cidx : columnIndexs) {
        outStreams.put(cidx, fs.create(new Path(outputPath, cidx + "/" + taskId), true));
      }
    }
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values,
        Context context) throws IOException, InterruptedException {
      String[] tokens = key.toString().split(DELIMETER);
      String distinctValue = tokens[0];
      int columnIndex = Integer.parseInt(tokens[1]);
      context.getCounter(DistinctColumnValuesJob.COUNT.TOTAL_COUNT).increment(1);
      outStreams.get(columnIndex).writeBytes(distinctValue + NEWLINE);
    }
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      for (Entry<Integer, FSDataOutputStream> e : outStreams.entrySet()) {
        e.getValue().close();
      }
    }
    
  }
}
