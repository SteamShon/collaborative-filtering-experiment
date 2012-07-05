package com.skp.experiment.cf.als.hama;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.VectorWritable;

public class MatrixMultiplication extends BSP<IntWritable, VectorWritable, IntWritable, VectorWritable, MatrixWritable> {
  private static int numRowsA;
  private static int numColsA;
  private static int numRowsB;
  private static int numColsB;
  private double[][] A;
  private double[][] B;
  private double[][] C;
  
  @Override
  public void cleanup(
      BSPPeer<IntWritable, VectorWritable, IntWritable, VectorWritable, MatrixWritable> peer)
      throws IOException {
    // TODO Auto-generated method stub
    super.cleanup(peer);
  }

  @Override
  public void setup(
      BSPPeer<IntWritable, VectorWritable, IntWritable, VectorWritable, MatrixWritable> peer)
      throws IOException, SyncException, InterruptedException {
    HamaConfiguration conf = (HamaConfiguration) peer.getConfiguration();
    numRowsA = conf.getInt("numRowsA", 0);
    numColsA = conf.getInt("numColsA", 0);
    numRowsB = numColsA;
    numColsB = conf.getInt("numColsB", 0);
    
  }
  
  private void initResultMatrix() {
    C = new double[numRowsA][numColsB];
    for (int i = 0; i < numRowsA; i++) {
      for (int j = 0; j < numColsB; j++) {
        C[i][j] = 0;
      }
    }
  }
  
  @Override
  public void bsp(
      BSPPeer<IntWritable, VectorWritable, IntWritable, VectorWritable, MatrixWritable> arg0)
      throws IOException, SyncException, InterruptedException {
    
  }

}
