package com.skp.experiment.cf.evaluate.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RecommendationsKeyComparator extends WritableComparator {
  private static double EPSILON = 0.000001;
  
  protected RecommendationsKeyComparator() {
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
    } else if (Math.abs(key1.getRate() - key2.getRate()) >= EPSILON) {
      return key1.getRate() < key2.getRate() ? 1 : -1;
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
    } else if (Math.abs(key1.getRate() - key2.getRate()) >= EPSILON) {
      return key1.getRate() < key2.getRate() ? 1 : -1;
    } else {
      return 0;
    }
  }
}
