package com.skp.experiment.cf.math.hadoop;

import static org.mockito.Matchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Test;

public class TestMatrixRowNormalizeMapper {
  private Vector createTestVector(int n) {
    Vector testVector = new RandomAccessSparseVector(10, 10);
    Random r = new Random();
    for (int i = 0; i < n; i++) {
      testVector.set(i, i);
    }
    return testVector;
  }
  

}
