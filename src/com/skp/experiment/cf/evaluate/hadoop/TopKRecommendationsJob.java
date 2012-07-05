package com.skp.experiment.cf.evaluate.hadoop;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.cf.taste.common.TopK;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;
import com.google.common.primitives.Floats;

public class TopKRecommendationsJob extends AbstractJob {
  private static final String RECOMMENDATIONS_PER_USER = "recommendationsPerUser";
  private static final String DELIMETER = ",";
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(RECOMMENDATIONS_PER_USER, "k", "recommendations per user.");
    
    if (parseArguments(args) == null) {
      return -1;
    }
    
    Job job = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class, 
        TopKRecommendationsMapper.class, IntWritable.class, Text.class, 
        SequenceFileOutputFormat.class);
    
    Configuration conf = job.getConfiguration();
    conf.setInt(RECOMMENDATIONS_PER_USER, Integer.parseInt(getOption(RECOMMENDATIONS_PER_USER)));
    job.waitForCompletion(true);
    return 0;
  }
  private static final Comparator<RecommendedItem> BY_PREFERENCE_VALUE =
      new Comparator<RecommendedItem>() {
    @Override
    public int compare(RecommendedItem one, RecommendedItem two) {
      return Floats.compare(one.getValue(), two.getValue());
    }
  };
  public static class TopKRecommendationsMapper extends 
    Mapper<IntWritable, VectorWritable, IntWritable, Text> {
    private static Text outValue = new Text();
    private static int recommendationsPerUser = 0;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      recommendationsPerUser = conf.getInt(RECOMMENDATIONS_PER_USER, 0);
    }

    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
        throws IOException, InterruptedException {
      Vector v = value.get();
      Iterator<Vector.Element> iter = v.iterateNonZero();
      TopK<RecommendedItem> topKItems = new TopK<RecommendedItem>(recommendationsPerUser, BY_PREFERENCE_VALUE);
      while (iter.hasNext()) {
        Vector.Element e = iter.next();
        topKItems.offer(new GenericRecommendedItem(e.index(), (float)e.get()));
      }
      List<RecommendedItem> recommendedItems = Lists.newArrayListWithExpectedSize(recommendationsPerUser);
      for (RecommendedItem topItem : topKItems.retrieve()) {
        recommendedItems.add(new GenericRecommendedItem(topItem.getItemID(), topItem.getValue()));
      }
      if (recommendedItems.size() > 0) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < recommendedItems.size(); i++) {
          RecommendedItem item = recommendedItems.get(i);
          if (i != 0) {
            sb.append(DELIMETER);
          }
          sb.append(item.getItemID()).append(DELIMETER).append(item.getValue());
        }
        outValue.set(sb.toString());
        context.write(key, outValue);
      }
    }
    
  }
  
}
