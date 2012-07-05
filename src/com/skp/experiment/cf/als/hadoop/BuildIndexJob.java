package com.skp.experiment.cf.als.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.AbstractJob;

import com.skp.experiment.common.OptionParseUtil;

public class BuildIndexJob extends AbstractJob {
  private static final String TGT_COLUMES = BuildIndexJob.class.getName() + ".targetColumns";
  private static final String DELIMETER = ",";
  @Override
  public int run(String[] args) throws Exception {
    return 0;
  }
  public static class DistinctMapper extends 
    Mapper<LongWritable, Text, IntWritable, Text> {
    
    private static List<Integer> targetColumns = new ArrayList<Integer>();
    private static IntWritable outKey = new IntWritable();
    private static Text outValue = new Text();
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      targetColumns = OptionParseUtil.decode(context.getConfiguration().get(TGT_COLUMES), DELIMETER);
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException,
    InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(line.toString());
      for (Integer column : targetColumns) {
        
      }
    }
  }

}
