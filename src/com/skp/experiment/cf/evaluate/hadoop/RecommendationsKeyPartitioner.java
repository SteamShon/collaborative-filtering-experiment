package com.skp.experiment.cf.evaluate.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RecommendationsKeyPartitioner extends Partitioner<RecommendationsKey, Text> {
  @Override
  public int getPartition(RecommendationsKey key, Text value,
      int numPartitions) {
    //return key.getUserID() % numPartitions;
    return Math.abs(key.getUserID().hashCode()) % numPartitions;
  }

}
