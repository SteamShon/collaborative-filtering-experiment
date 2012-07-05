package com.skp.experiment.common.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeJoinKeyGroupingComparator extends WritableComparator {

  protected CompositeJoinKeyGroupingComparator() {
    super(CompositeJoinKey.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    CompositeJoinKey key1 = (CompositeJoinKey) a;
    CompositeJoinKey key2 = (CompositeJoinKey) b;
    return key1.getJoinKey().compareTo(key2.getJoinKey());
  }
  
}
