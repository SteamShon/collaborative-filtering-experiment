package com.skp.experiment.cf.als.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.cf.taste.common.TopK;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.FileLineIterator;
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
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenHashMap;
import org.apache.mahout.math.map.OpenIntObjectHashMap;

import com.google.common.primitives.Floats;
import com.skp.experiment.common.OptionParseUtil;
/**
 * This is helper utils for DistributedRowMatrix. mainly stolen from mahout. 
 * @author doyoung
 *
 */
public class ALSMatrixUtil {
  /**
   * read first row of matrix as Vector.
   * @param dir, HDFS path for DistributedRowMatrix(<IntWritable, VectorWritable> SequenceFile) reside on.
   * @param conf, Job Configuration
   * @return Vector, first row of DistributedRowMatrix without IntWritable(key).
   * @throws IOException
   */
  public static Vector readFirstRow(Path dir, Configuration conf) throws IOException {
    Iterator<VectorWritable> iterator = new SequenceFileDirValueIterator<VectorWritable>(dir,
        PathType.LIST,
        PathFilters.partFilter(),
        null,
        true,
        conf);
    return iterator.hasNext() ? iterator.next().get() : null;
  }
  
  /**
   * read entire matrix into memory using row_id as key.
   */
  @SuppressWarnings("rawtypes")
  public static OpenIntObjectHashMap<Vector> readMatrixByRows(Path dir, Context ctx) {
    return readMatrixByRows(dir, ctx.getConfiguration());
  }
  
  /**
   * read entire matrix into memory using row_id as key from Mapper`s setup().
   * caution, caller of this usually in Mapper,  so when source matrix is large map task usually runs out of heap space.
   * to address this make sure caller force only proper number of map task instance runs simultaneously on each datanode.
   * (I have tried file lock on HDFS since it is easy and good enough for my needs but will try with Zookeeper later)
   * @param dir, HDFS path for DistributedRowMatrix.
   * @param conf, Configuration of this cluster. 
   * @return Hash<Integer, Vector>, whole file based DistributedRowMatrix rows.key is row_id
   */
  public static OpenIntObjectHashMap<Vector> readMatrixByRows(Path dir, Configuration conf) {
    OpenIntObjectHashMap<Vector> matrix = new OpenIntObjectHashMap<Vector>();
    for (Pair<IntWritable,VectorWritable> pair :
        new SequenceFileDirIterable<IntWritable,VectorWritable>(dir, PathType.LIST, PathFilters.partFilter(), conf)) {
      int rowIndex = pair.getFirst().get();
      Vector row = pair.getSecond().get().clone();
      matrix.put(rowIndex, row);
    }
    return matrix;
  }
  
  public static Matrix conv2Matrix(OpenHashMap<Integer, Vector> x, int rowNums, int colNums) {
    Matrix matrix = new DenseMatrix(rowNums, colNums);
    for (Integer rowID : x.keySet()) {
      matrix.assignRow(rowID, x.get(rowID));
    }
    return matrix;
  }
  
  
  /**
   * read entire matrix into memory using row_id as key from Mapper`s setup(). 
   * use this when caller knows dimention of source matrix.
   */
  public static DenseMatrix readDenseMatrixByRows(Path dir, Context ctx, int numRows, int numCols) {
    return readDenseMatrixByRows(dir, ctx.getConfiguration(), numRows, numCols);
  }
  
  public static DenseMatrix readDenseMatrixByRows(Path dir, Configuration conf, int numRows, int numCols) {
    DenseMatrix matrix = new DenseMatrix(numRows, numCols);
    
    for (Pair<IntWritable, VectorWritable> pair: 
      new SequenceFileDirIterable<IntWritable, VectorWritable>(dir, PathType.LIST, PathFilters.partFilter(), conf)) {
      int rowIndex = pair.getFirst().get();
      Vector row = pair.getSecond().get().clone();
      matrix.assignRow(rowIndex, row);
    }
    return matrix;
  }
  
  /**
   * Comparator class to sort RecommendedItem by score
   */
  public static final Comparator<RecommendedItem> BY_PREFERENCE_VALUE =
      new Comparator<RecommendedItem>() {
    @Override
    public int compare(RecommendedItem one, RecommendedItem two) {
      return Floats.compare(one.getValue(), two.getValue());
    }
  }; 
      
  /**
   * extract topK RecommendedItem sorted by BY_PREFERENCE_VALUE
   * @param dir, HDFS path for DistributedRowMatrix
   * @param ctx, job`s Context
   * @param topK, topK items will be kept in priority_queue
   * @return Map<Integer, TopK<RecommendedItem> >, topK score items per each row_id
   */
  @SuppressWarnings("rawtypes")
  public static OpenIntObjectHashMap<TopK<RecommendedItem>> readMatrixByRowsTopK(Path dir, Context ctx, int topK) {
    return readMatrixByRowsTopK(dir, ctx.getConfiguration(), topK);
  }
  
  public static OpenIntObjectHashMap<TopK<RecommendedItem>> readMatrixByRowsTopK(Path dir, Configuration conf, int topK) {
    OpenIntObjectHashMap<TopK<RecommendedItem>> matrix = new OpenIntObjectHashMap<TopK<RecommendedItem>>();
    for (Pair<IntWritable, VectorWritable> pair : 
      new SequenceFileDirIterable<IntWritable, VectorWritable>(new Path(dir.toString() + "/^[^_]*"), PathType.GLOB, null, conf)) {
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
  
  public static Matrix readDistributedRowMatrix(Path dir, Configuration conf, int numRows, int numCols) {
    Matrix result = new SparseMatrix(numRows, numCols);
    DistributedRowMatrix X = new DistributedRowMatrix(dir, new Path("/tmp/multiply"), numRows, numCols);
    X.setConf(conf);
    Iterator<MatrixSlice> rows = X.iterator();
    while (rows.hasNext()) {
      MatrixSlice row = rows.next();
      result.assignRow(row.index(), row.vector());
    }
    return result;
  }
  public static Matrix readDistributedRowMatrix(Path dir, int numRows, int numCols) {
    return readDistributedRowMatrix(dir, new Configuration(), numRows, numCols);
  }
  
  
  
  /**
   * fetch key value hashmap using column index list as key, value
   * @param conf 
   * @param input
   * @param delimeter
   * @param keyIdxs
   * @param valueIdxs
   * @return
   * @throws IOException
   */
  public static Map<String, String> fetchTextFiles(Configuration conf, Path input, String delimeter,
      List<Integer> keyIdxs, List<Integer> valueIdxs) throws IOException {
    Map<String, String> caches = new HashMap<String, String>();
    // read target file.
    FileSystem fs = FileSystem.get(conf);
    if (fs.isFile(input)) {
      fetchTextFile(fs.open(input), delimeter, keyIdxs, valueIdxs, caches);
    } else {
      FileStatus[] files = fs.globStatus(new Path(input.toString() + "/^[^_]*"));
      for (FileStatus file : files) {
        fetchTextFile(fs.open(file.getPath()), delimeter, keyIdxs, valueIdxs, caches);
      }
    }
    return caches;
  }
  public static void fetchTextFile(FSDataInputStream in, String delimeter,
      List<Integer> keyIdxs, List<Integer> valueIdxs, Map<String, String> result) throws IOException {
    FileLineIterator iter = new FileLineIterator(in);
    while (iter.hasNext()) {
      String line = iter.next();
      String[] tokens = line.split(delimeter);
      // record target key, value
      String key = OptionParseUtil.encode(tokens, keyIdxs, delimeter);
      String value = OptionParseUtil.encode(tokens, valueIdxs, delimeter);
      result.put(key, value);
    }
  }
  
  @SuppressWarnings("rawtypes")
  public static Map<String, String> fetchTextFiles(Context ctx, Path input, String delimeter,
      List<Integer> keyIdxs, List<Integer> valueIdxs) throws IOException {
    return fetchTextFiles(ctx.getConfiguration(), input, delimeter, keyIdxs, valueIdxs);
  }
  public static Map<String, String> fetchTextFiles(Path input, String delimeter,
      List<Integer> keyIdxs, List<Integer> valueIdxs) throws IOException {
    return fetchTextFiles(new Configuration(), input, delimeter, keyIdxs, valueIdxs);
  }
}
