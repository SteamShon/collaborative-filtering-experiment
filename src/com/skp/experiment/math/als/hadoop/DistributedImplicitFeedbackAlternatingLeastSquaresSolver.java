/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.skp.experiment.math.als.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.QRDecomposition;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.map.OpenIntObjectHashMap;

import com.google.common.base.Preconditions;

/** see <a href="http://research.yahoo.com/pub/2433">Collaborative Filtering for Implicit Feedback Datasets</a> */
public class DistributedImplicitFeedbackAlternatingLeastSquaresSolver {
  private final int numRows;
  private final int numFeatures;
  private final double alpha;
  private final double lambda;
  //private final long maxMatrixSize = 1024 * 1024 * 10;
  private final double maxCacheRatio = 0.95;
  
  //private final OpenIntObjectHashMap<Vector> Y;
  //private DistributedRowMatrix Y;
  private Matrix YtransposeY;
  private MapFile.Reader reader;
  private OpenIntObjectHashMap<Vector> sparseY;
  private Map<Integer, MapFile.Reader> mapFileReaders;
  
  
  public DistributedImplicitFeedbackAlternatingLeastSquaresSolver(int numRows, int numFeatures, double lambda, double alpha,
      MapFile.Reader reader, Matrix YtransposeY) {
    this.numRows = numRows;
    this.numFeatures = numFeatures;
    this.lambda = lambda;
    this.alpha = alpha;
    this.YtransposeY = YtransposeY;
    this.reader = reader;
    this.sparseY = new OpenIntObjectHashMap<Vector>(this.numRows);
    //this.Y = Y;
    //YtransposeY = YtransposeY(Y);
  }
  
  public void setMapFileReaders(Map<Integer, MapFile.Reader> mapFileReaders) {
    this.mapFileReaders = mapFileReaders;
  }
  
  private static Vector solve(Matrix A, Matrix y) {
    return new QRDecomposition(A).solve(y).viewColumn(0);
  }

  protected double confidence(double rating) {
    return 1 + alpha * rating;
  }
  
  private boolean needReset() {
    long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    double usedRatio = (double) usedMemory / Runtime.getRuntime().totalMemory();
    if (usedRatio > maxCacheRatio) {
      return true;
    }
    return false;
  }
  
  private int getPartition(int index) {
    return index % mapFileReaders.size();
  }
  
  private Vector retrieveRow(int index) throws IOException {
    IntWritable rowIDWritable = new IntWritable(index);
    VectorWritable colWritable = new VectorWritable();
    /*
    if (reader.get(rowIDWritable, colWritable) == null) {
      throw new IOException("find " + index + " in MapFile failed!");
    }
    */
    
    if (mapFileReaders.get(getPartition(index)).get(rowIDWritable, colWritable) == null) {
      throw new IOException("find " + index + " in MapFile failed!");
    }
    return colWritable.get();
  }
  
  private Vector getMatrixRow(int index) throws IOException {
    if (needReset()) {
      sparseY.clear();
    }
    if (sparseY.containsKey(index)) {
      return sparseY.get(index);
    }
    // cache
    sparseY.put(index, retrieveRow(index));
    return sparseY.get(index);
  }
  
  /** get only necessary part of Y matrix 
   * @throws IOException */
  private void getSparseMatrix(Vector userRatings) throws IOException {
    Iterator<Vector.Element> ratings = userRatings.iterateNonZero();
    while (ratings.hasNext()) {
      Vector.Element e = ratings.next();
      getMatrixRow(e.index());
    }
  }
  
  
  
  public Vector solve(Vector userRatings) throws IOException {
    Preconditions.checkArgument(userRatings.isSequentialAccess(), "need sequential access to ratings!");
    //Matrix sparseY = getSparseMatrix(userRatings);
    getSparseMatrix(userRatings);
    /* Y' (Cu - I) Y + λ I */
    /* Y' Cu p(u) */
    Vector YtransponseCuPu = new DenseVector(numFeatures);
    /* (Cu -I) Y */
    OpenIntObjectHashMap<Vector> CuMinusIY = new OpenIntObjectHashMap<Vector>();
    
    
    Iterator<Vector.Element> ratings = userRatings.iterateNonZero();
    while (ratings.hasNext()) {
      Vector.Element e = ratings.next();
      CuMinusIY.put(e.index(), sparseY.get(e.index()).times(confidence(e.get()) - 1));
      /* Y' Cu p(u) */
      YtransponseCuPu.assign(sparseY.get(e.index()).times(confidence(e.get())), Functions.PLUS);
    }

    Matrix YtransponseCuMinusIY = new DenseMatrix(numFeatures, numFeatures);

    /* Y' (Cu -I) Y by outer products */
    ratings = userRatings.iterateNonZero();
    while (ratings.hasNext()) {
      Vector.Element e = ratings.next();
      for (Vector.Element feature : sparseY.get(e.index())) {
        Vector partial = CuMinusIY.get(e.index()).times(feature.get());
        YtransponseCuMinusIY.viewRow(feature.index()).assign(partial, Functions.PLUS);
      }
    }

    /* Y' (Cu - I) Y + λ I  add lambda on the diagonal */
    for (int feature = 0; feature < numFeatures; feature++) {
      YtransponseCuMinusIY.setQuick(feature, feature, YtransponseCuMinusIY.getQuick(feature, feature) + lambda);
    }
    
    Matrix YtransposeCuPu = columnVectorAsMatrix(YtransponseCuPu);
    return solve(YtransposeY.plus(YtransponseCuMinusIY), YtransposeCuPu);
    //return YtransponseCuMinusIY;
  }
  /*
  // Y' Cu p(u) //
  private Matrix YtransponseCuPu(Vector userRatings) {
    Preconditions.checkArgument(userRatings.isSequentialAccess(), "need sequential access to ratings!");

    Vector YtransponseCuPu = new DenseVector(numFeatures);

    Iterator<Vector.Element> ratings = userRatings.iterateNonZero();
    while (ratings.hasNext()) {
      Vector.Element e = ratings.next();
      YtransponseCuPu.assign(Y.get(e.index()).times(confidence(e.get())), Functions.PLUS);
    }

    return columnVectorAsMatrix(YtransponseCuPu);
  }
  */
  private Matrix columnVectorAsMatrix(Vector v) {
    Matrix matrix = new DenseMatrix(numFeatures, 1);
    for (Vector.Element e : v) {
      matrix.setQuick(e.index(), 0, e.get());
    }
    return matrix;
  }
 
}
