package com.skp.experiment.common.join;

import org.apache.hadoop.mapreduce.Partitioner;

public class CompositeJoinKeyPartitioner extends Partitioner<CompositeJoinKey, CompositeJoinValue>{
  @Override
  public int getPartition(CompositeJoinKey key,CompositeJoinValue value, int numPartitions) {
    int hashCode = Math.abs(key.getJoinKey().hashCode());
    return Math.max(0,hashCode % numPartitions);
  }
}
