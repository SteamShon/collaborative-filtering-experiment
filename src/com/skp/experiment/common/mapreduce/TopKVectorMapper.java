package com.skp.experiment.common.mapreduce;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.common.TopK;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;
import com.google.common.primitives.Floats;

public class TopKVectorMapper extends 
  Mapper<IntWritable, VectorWritable, NullWritable, Text> {
  public static String TOP_K = TopKVectorMapper.class.getName() + ".topK";
  private static int topK;
  private static Text outValue = new Text();
  private static final String DELIMETER = ",";
  
  public static final Comparator<RecommendedItem> BY_PREFERENCE_VALUE =
      new Comparator<RecommendedItem>() {
        @Override
        public int compare(RecommendedItem one, RecommendedItem two) {
          return Floats.compare(one.getValue(), two.getValue());
        }
      };
      
  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    topK = context.getConfiguration().getInt(TOP_K, 0);
  }
  @Override
  protected void map(IntWritable key, VectorWritable value, Context context) throws IOException,
      InterruptedException {
    final Vector cols = value.get();
    final TopK<RecommendedItem> topKItems = 
        new TopK<RecommendedItem>(topK, BY_PREFERENCE_VALUE);
    Iterator<Vector.Element> iter = cols.iterateNonZero();
    while (iter.hasNext()) {
      Vector.Element e = iter.next();
      topKItems.offer(new GenericRecommendedItem(e.index(), (float)e.get()));
    }
    //List<RecommendedItem> recommendedItems = Lists.newArrayListWithExpectedSize(topK);
    for (RecommendedItem topItem : topKItems.retrieve()) {
      //recommendedItems.add(new GenericRecommendedItem(topItem.getItemID(), topItem.getValue()));
      outValue.set(key.get() + DELIMETER + topItem.getItemID() + DELIMETER + topItem.getValue());
      context.write(NullWritable.get(), outValue);
    }
    //context.write(key, new RecommendedItemsWritable(recommendedItems));
  }

  
  
}
