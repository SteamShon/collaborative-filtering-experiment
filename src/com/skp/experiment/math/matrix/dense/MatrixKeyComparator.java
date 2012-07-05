package com.skp.experiment.math.matrix.dense;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class MatrixKeyComparator extends WritableComparator {

  protected MatrixKeyComparator() {
    super(MatrixKey.class, true);
  }
  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    MatrixKey key1 = (MatrixKey) a;
    MatrixKey key2 = (MatrixKey) b;
    return key1.compareTo(key2);
  }
}
