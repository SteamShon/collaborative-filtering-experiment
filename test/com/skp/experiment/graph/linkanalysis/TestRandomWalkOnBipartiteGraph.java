package com.skp.experiment.graph.linkanalysis;

import static org.junit.Assert.fail;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.junit.Test;

public class TestRandomWalkOnBipartiteGraph {
  private int numItems;
  private int numUsers;
  
  private Matrix E = new SparseMatrix(numUsers, numItems);
  private Matrix Etranspose = E.transpose();
  
  @Test
  public void test() {
    fail("Not yet implemented");
  }

}
