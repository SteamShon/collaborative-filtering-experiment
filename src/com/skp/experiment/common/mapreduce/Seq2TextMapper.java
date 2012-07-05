package com.skp.experiment.common.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Seq2TextMapper<K1, V1> extends Mapper<K1, V1, NullWritable, Text> {
  private static Text outValue = new Text();
  private static final String DELIMETER = ",";
  @Override
  protected void map(K1 key, V1 value,Context context) 
      throws IOException, InterruptedException {
    outValue.set(key.toString() + DELIMETER + value.toString());
    context.write(NullWritable.get(), outValue);
  }
  
}
