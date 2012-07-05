package com.skp.experiment.math.matrix.dense;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MatrixKeyGroupingComparator extends WritableComparator {

  protected MatrixKeyGroupingComparator() {
    super(MatrixKey.class, true);
  }
  
  public int compare(WritableComparable a, WritableComparable b) {
    MatrixKey key1 = (MatrixKey) a;
    MatrixKey key2 = (MatrixKey) b;
    if (key1.getIndex1() < key2.getIndex1()) {
      return -1;
    } else if (key1.getIndex1() > key2.getIndex1()) {
      return +1;
    }
    if (key1.getIndex2() < key2.getIndex2()) {
      return -1;
    } else if (key1.getIndex2() > key2.getIndex2()) {
      return +1;
    }
    return 0;
  }
}
