package com.skp.experiment.cf.math.hadoop;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DistributedRowMatrixWriter;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.junit.Test;

public class TestDistributedRowMatrixTestUtil {
  private DistributedRowMatrixTestUtil util;
  
  @Test
  public void testConvert2DistributedRowMatrix() throws IOException {
    Configuration conf = new Configuration();
    int numRows = 3, numCols = 3;
    Random r = new Random();
    double[][] m = new double[numRows][numCols];
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numCols; j++) {
        m[i][j] = r.nextDouble();
      }
    }
    
    Matrix matrix = new DenseMatrix(m);
    DistributedRowMatrixWriter.write(new Path("data/test/distMatrix/test.in"), conf, matrix);
    DistributedRowMatrix distMatrix = new DistributedRowMatrix(new Path("data/test/distMatrix"), 
        new Path("data/test/distMatrix_tmp"), numRows, numCols);
    distMatrix.setConf(conf);
    
    Iterator<MatrixSlice> iterator = distMatrix.iterateAll();
    while (iterator.hasNext()) {
      MatrixSlice cur = iterator.next();
      int rowID = cur.index();
      Vector cols = cur.vector();
      Iterator<Vector.Element> columns = cols.iterator();
      while (columns.hasNext()) {
        Vector.Element e = columns.next();
        assertTrue(Math.abs(e.get() - m[rowID][e.index()]) < util.EPSILON);
      }
    }
  }

}
