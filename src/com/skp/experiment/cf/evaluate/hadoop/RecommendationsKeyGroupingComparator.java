package com.skp.experiment.cf.evaluate.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RecommendationsKeyGroupingComparator extends WritableComparator {

  protected RecommendationsKeyGroupingComparator() {
    super(RecommendationsKey.class, true);
  }
  /*
  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    RecommendationsKey key1 = (RecommendationsKey) a;
    RecommendationsKey key2 = (RecommendationsKey) b;
    if (key1.getUserID() != key2.getUserID()) {
      return key1.getUserID() < key2.getUserID() ? -1 : 1;
    } else {
      return 0;
    }
  }
  */
  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    RecommendationsKey key1 = (RecommendationsKey) a;
    RecommendationsKey key2 = (RecommendationsKey) b;
    if (key1.getUserID().compareTo(key2.getUserID()) != 0) {
      return key1.getUserID().compareTo(key2.getUserID());
    } else {
      return 0;
    }
  }
}
