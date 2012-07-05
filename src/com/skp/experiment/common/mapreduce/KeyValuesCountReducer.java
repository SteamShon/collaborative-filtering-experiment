package com.skp.experiment.common.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

@SuppressWarnings("rawtypes")
public class KeyValuesCountReducer extends
    Reducer<Text, IntWritable, WritableComparable, Writable> {
  
  public static final String OUTPUT_TYPE = KeyValuesCountReducer.class.getName() + ".outputType";
  public static final String DELIMETER_KEY = KeyValuesCountReducer.class.getName() + ".delimeter";
  private static IntWritable outValue = new IntWritable();
  private static Text outValueText = new Text();
  
  private static String outputType;
  private static String delimeter;
  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    
    // output type option{seq, text}
    outputType = context.getConfiguration().get(OUTPUT_TYPE).toLowerCase();
    if (outputType == null) {
      outputType = "seq";
    }
    delimeter = context.getConfiguration().get(DELIMETER_KEY).toLowerCase();
    if (delimeter == null) {
      delimeter = ",";
    }
  }

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable value : values) {
      sum += value.get();
    }
    if (outputType.equals("text")) {
      outValueText.set(key.toString() + delimeter + sum);
      context.write(NullWritable.get(), outValueText);
    } else {
      outValue.set(sum);
      context.write(key, outValue);
    }
    
  }

  
  
}
