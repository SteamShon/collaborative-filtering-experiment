package com.skp.experiment.common.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;

public class KeyValuesCountMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {
  public static final String KEY_COLUMN_INDEX = KeyValuesCountMapper.class.getName() + ".keyColumnIndex";
  private static int keyColumnIndex;
  private static Text outKey = new Text();
  private static IntWritable outValue = new IntWritable(1);
  
  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    keyColumnIndex = context.getConfiguration().getInt(KEY_COLUMN_INDEX, 0);
  }
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
    String[] tokens = TasteHadoopUtils.splitPrefTokens(value.toString());
    outKey.set(tokens[keyColumnIndex]);
    context.write(outKey, outValue);
  }
}
