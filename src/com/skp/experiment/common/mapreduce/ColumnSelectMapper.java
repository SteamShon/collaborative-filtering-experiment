package com.skp.experiment.common.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;

import com.skp.experiment.common.OptionParseUtil;

/**
 * select columns in order given by param
 * @author 1000668
 *
 */
public class ColumnSelectMapper extends
    Mapper<LongWritable, Text, NullWritable, Text> {
  public static final String COLUMN_INDEXS = ColumnSelectMapper.class.getName() + ".columnIndexs";
  public static final String TRANSFORM_INDEX = ColumnSelectMapper.class.getName() + ".transformIndex";
  
  private static final String DELIMETER = ",";
  private static List<Integer> columnIndexs;
  private static Text outValue = new Text();
  private static int transformIndex = -1;
  @Override
  protected void setup(Context context) 
      throws IOException, InterruptedException {
    columnIndexs = OptionParseUtil.decode(
        context.getConfiguration().get(COLUMN_INDEXS), DELIMETER);
    transformIndex = context.getConfiguration().getInt(TRANSFORM_INDEX, -1);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) 
      throws IOException, InterruptedException {
    String[] tokens = TasteHadoopUtils.splitPrefTokens(value.toString());
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < columnIndexs.size(); i++) {
      int idx = columnIndexs.get(i);
      if (idx < 0 || idx >= tokens.length) {
        continue;
      }
      if (i != 0) {
        sb.append(DELIMETER);
      }
      if (idx == transformIndex) {
        sb.append(Math.log(Float.parseFloat(tokens[idx]) + 1.0) + 1.0);
      } else {
        sb.append(tokens[idx]);
      }
    }
    outValue.set(sb.toString());
    context.write(NullWritable.get(), outValue);
  }
  
}
