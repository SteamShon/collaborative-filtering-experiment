package com.skp.experiment.cf.als.hadoop;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.cf.taste.common.TopK;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterator;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Floats;

public class ALSMatrixUtil {
  private static int reportThreshold = 100000;
  private static final Logger log = LoggerFactory.getLogger(ALSMatrixUtil.class);
  
  public static Vector readFirstRow(Path dir, Configuration conf) throws IOException {
    Iterator<VectorWritable> iterator = new SequenceFileDirValueIterator<VectorWritable>(dir,
                                                                                         PathType.LIST,
                                                                                         PathFilters.partFilter(),
                                                                                         null,
                                                                                         true,
                                                                                         conf);
    return iterator.hasNext() ? iterator.next().get() : null;
  }
  
  @SuppressWarnings("rawtypes")
  public static OpenIntObjectHashMap<Vector> readMatrixByRows(Path dir, Context ctx) {
    long memoryUsed = Runtime.getRuntime().freeMemory();
    Configuration conf = ctx.getConfiguration();
    OpenIntObjectHashMap<Vector> matrix = new OpenIntObjectHashMap<Vector>();
    int accumulatedCnt = 0;
    long timeDuration = System.currentTimeMillis();
    for (Pair<IntWritable,VectorWritable> pair :
        new SequenceFileDirIterable<IntWritable,VectorWritable>(dir, PathType.LIST, PathFilters.partFilter(), conf)) {
      int rowIndex = pair.getFirst().get();
      Vector row = pair.getSecond().get().clone();
      matrix.put(rowIndex, row);
      accumulatedCnt++;
      if (accumulatedCnt % reportThreshold == 0) {
        ctx.setStatus("fetching " + accumulatedCnt + " so far..");
      }
    }
    memoryUsed = Runtime.getRuntime().freeMemory() - memoryUsed;
    timeDuration = System.currentTimeMillis() - timeDuration;
    ctx.setStatus("total fetch: " + matrix.size() + "\ttotal memory: " + memoryUsed + "\tTime: " + timeDuration);
    return matrix;
  }
  public static Matrix readMatrixByRows(Path dir, Context ctx, int numRows, int numCols) {
    long memoryUsed = Runtime.getRuntime().freeMemory();
    long timeDuration = System.currentTimeMillis();
    Configuration conf = ctx.getConfiguration();
    Matrix matrix = new DenseMatrix(numRows, numCols);
    int accumulatedCnt = 0;
    for (Pair<IntWritable, VectorWritable> pair: 
      new SequenceFileDirIterable<IntWritable, VectorWritable>(dir, PathType.LIST, PathFilters.partFilter(), conf)) {
      int rowIndex = pair.getFirst().get();
      Vector row = pair.getSecond().get().clone();
      matrix.assignRow(rowIndex, row);
      accumulatedCnt++;
      if (accumulatedCnt % reportThreshold == 0) {
        ctx.setStatus("fetching " + accumulatedCnt + " so far..");
      }
    }
    memoryUsed = Runtime.getRuntime().freeMemory() - memoryUsed;
    timeDuration = System.currentTimeMillis() - timeDuration;
    ctx.setStatus("total fetch: " + matrix.numRows() + "\ttotal memory: " + memoryUsed + "\tTime: " + timeDuration);
    return matrix;
  }
  public static OpenIntObjectHashMap<Vector> readMatrixByRows(Path dir, Configuration conf) {
    long memoryUsed = Runtime.getRuntime().freeMemory();
    OpenIntObjectHashMap<Vector> matrix = new OpenIntObjectHashMap<Vector>();
    int accumulatedCnt = 0;
    for (Pair<IntWritable,VectorWritable> pair :
        new SequenceFileDirIterable<IntWritable,VectorWritable>(dir, PathType.LIST, PathFilters.partFilter(), conf)) {
      int rowIndex = pair.getFirst().get();
      Vector row = pair.getSecond().get().clone();
      matrix.put(rowIndex, row);
      accumulatedCnt++;
      if (accumulatedCnt % reportThreshold == 0) {
        log.info("fetching {} so far...", accumulatedCnt);
      }
    }
    memoryUsed = Runtime.getRuntime().freeMemory() - memoryUsed;
    log.info("total fetch: {}\tTotal Memory: {}", matrix.size(), memoryUsed);
    return matrix;
  }
  @SuppressWarnings("rawtypes")
  public static OpenIntObjectHashMap<Vector> readMatrixByRowsAll(Path dir, Context ctx) {
    long memoryUsed = Runtime.getRuntime().freeMemory();
    Configuration conf = ctx.getConfiguration();
    OpenIntObjectHashMap<Vector> matrix = new OpenIntObjectHashMap<Vector>();
    int accumulatedCnt = 0;
    for (Pair<IntWritable,VectorWritable> pair :
        new SequenceFileDirIterable<IntWritable,VectorWritable>(new Path(dir.toString() + "/^[^_]*"), PathType.GLOB, null, conf)) {
      matrix.put(pair.getFirst().get(), pair.getSecond().get());
      accumulatedCnt++;
      if (accumulatedCnt % reportThreshold == 0) {
        ctx.setStatus("fetching " + accumulatedCnt + " so far..");
      }
    }
    memoryUsed = Runtime.getRuntime().freeMemory() - memoryUsed;
    ctx.setStatus("total fetch: " + matrix.size() + "\tMemory Used: " + memoryUsed);
    return matrix;
  }
  
  public static final Comparator<RecommendedItem> BY_PREFERENCE_VALUE =
      new Comparator<RecommendedItem>() {
    @Override
    public int compare(RecommendedItem one, RecommendedItem two) {
      return Floats.compare(one.getValue(), two.getValue());
    }
  }; 
      
  @SuppressWarnings("rawtypes")
  public static OpenIntObjectHashMap<TopK<RecommendedItem>>
    readMatrixByRowsInOrder(Path dir, Context ctx, int topK) {
    OpenIntObjectHashMap<TopK<RecommendedItem>> matrix = 
        new OpenIntObjectHashMap<TopK<RecommendedItem>>();
    Configuration conf = ctx.getConfiguration();
    int count = 0;
    for (Pair<IntWritable, VectorWritable> pair : 
      new SequenceFileDirIterable<IntWritable, VectorWritable>(new Path(dir.toString() + "/^[^_]*"), PathType.GLOB, null, conf)) {
      if (count++ % (reportThreshold / 100) == 0) {
        ctx.setStatus("fetched so far: " + count);
      }
      TopK<RecommendedItem> topKItems = new TopK<RecommendedItem>(topK, BY_PREFERENCE_VALUE);
      int rowIndex = pair.getFirst().get();
      Vector rowVector = pair.getSecond().get().clone();
      Iterator<Vector.Element> cols = rowVector.iterateNonZero();
      while (cols.hasNext()) {
        Vector.Element e = cols.next();
        topKItems.offer(new GenericRecommendedItem(e.index(), (float)e.get()));
      }
      matrix.put(rowIndex, topKItems);
    }
    return matrix;
  }
  
  public static Matrix retrieveDistributedRowMatrix(Path dir, int numRows, int numCols) {
    Matrix result = new SparseMatrix(numRows, numCols);
    DistributedRowMatrix X = new DistributedRowMatrix(dir, new Path("/tmp/multiply"), numRows, numCols);
    X.setConf(new Configuration());
    Iterator<MatrixSlice> rows = X.iterator();
    while (rows.hasNext()) {
      MatrixSlice row = rows.next();
      result.assignRow(row.index(), row.vector());
    }
    return result;
  }
  
  
}
