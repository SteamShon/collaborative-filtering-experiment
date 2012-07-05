package com.skp.experiment.common.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;

import com.skp.experiment.common.OptionParseUtil;
/**
 * group by TextInputFileFormat.
 * @author skplanet
 *
 */
public class GroupByColumnInValueMapper 
  extends Mapper<LongWritable, Text, Text, Text> {
  
  public static final String KEY_COLUMN_INDEX = GroupByColumnInValueMapper.class.getName() + ".keyColumnIndex";
  public static final String DELIMETER = ",";
  private List<Integer> keyColumnIndexs;
  private static Text outKey = new Text();
  
  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    keyColumnIndexs = 
        OptionParseUtil.decode(context.getConfiguration().get(KEY_COLUMN_INDEX), DELIMETER);
  }
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
    String[] tokens = TasteHadoopUtils.splitPrefTokens(value.toString());
    outKey.set(OptionParseUtil.encode(tokens, keyColumnIndexs, DELIMETER));
    context.write(outKey, value);
  }
}
