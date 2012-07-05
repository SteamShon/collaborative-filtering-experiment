package com.skp.experiment.common.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeJoinKeyComparator extends WritableComparator {

  protected CompositeJoinKeyComparator() {
    super(CompositeJoinKey.class, true);
  }
  
  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    CompositeJoinKey key1 = (CompositeJoinKey) a;
    CompositeJoinKey key2 = (CompositeJoinKey) b;
    int cmp = key1.getJoinKey().compareTo(key2.getJoinKey());
    if (cmp != 0) {
      return cmp;
    }
    return key1.getSuffix() == key2.getSuffix() ? 0 : 
      (key1.getSuffix() < key2.getSuffix() ? -1 : 1);
  }
}
