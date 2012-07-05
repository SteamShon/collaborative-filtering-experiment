package com.skp.experiment.cf.als.hama;

public class MatMult {
  private static double[][] A;
  private static double[][] B;
  private static double[][] C;
  private static int N;
  
  
  public static class MatMultThread extends Thread {
    int row;
    MatMultThread(int i) {
      row = i;
      this.start();
    }
    public void run() {
      for (int i = 0; i < N; i++) {
        C[row][i] = 0;
        for (int j = 0; j < N; j++) {
          C[row][i] += A[row][j] * B[j][i];
        }
      }
    }
  }
  
  public static void Multiply(double[][] a, double[][] b, double[][] c, int numRowsA, int numColsA, int numColsB) 
      throws InterruptedException {
    N = numColsA;
    A = a;
    B = b;
    C = c;
    MatMultThread threads[] = new MatMultThread[numRowsA];
    for (int i = 0; i < numRowsA; i++) {
      threads[i] = new MatMultThread(i);
    }
    for (int i = 0; i < numRowsA; i++) {
      threads[i].join();
    }
  }
}
