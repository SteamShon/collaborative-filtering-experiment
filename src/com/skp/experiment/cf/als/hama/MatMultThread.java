package com.skp.experiment.cf.als.hama;

public class MatMultThread extends Thread {
  static double A[][];
  static double B[][];
  static double C[][];
  static int N;
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
