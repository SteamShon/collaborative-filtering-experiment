package com.skp.experiment.common;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import com.skp.experiment.common.Text2DistributedRowMatrixJob.Text2DistributedRowMatrixMapper;
import com.skp.experiment.common.Text2DistributedRowMatrixJob.Text2DistributedRowMatrixReducer;
public class Text2DistributedRowMatrixJobTest extends MahoutTestCase{
  
  Text2DistributedRowMatrixMapper mapper = new Text2DistributedRowMatrixMapper();
  Text2DistributedRowMatrixReducer reducer = new Text2DistributedRowMatrixReducer();
  
  @SuppressWarnings("rawtypes")
  MapDriver<LongWritable, Text, WritableComparable, VectorWritable> mapDriver;
  @SuppressWarnings("rawtypes")
  ReduceDriver<WritableComparable, VectorWritable, WritableComparable, VectorWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, IntWritable, VectorWritable, IntWritable, VectorWritable> mapReduceDriver;
  
  @SuppressWarnings("rawtypes")
  @Before
  public void setUp() throws Exception {
    super.setUp();
    mapDriver = new MapDriver<LongWritable, Text, WritableComparable, VectorWritable>();
    reduceDriver = new ReduceDriver<WritableComparable, VectorWritable, WritableComparable, VectorWritable>();
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, IntWritable, VectorWritable, IntWritable, VectorWritable>();
    mapDriver.setMapper(mapper);
    reduceDriver.setReducer(reducer);
  }
  
  @SuppressWarnings("rawtypes")
  @Test
  public void testMapper() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(Text2DistributedRowMatrixJob.ROW_IDX_KEY, 0);
    conf.setInt(Text2DistributedRowMatrixJob.COL_IDX_KEY, 1);
    conf.setInt(Text2DistributedRowMatrixJob.VALUE_IDX_KEY, 2);
    conf.setBoolean(Text2DistributedRowMatrixJob.SEQUENTIAL, false);
    conf.set(Text2DistributedRowMatrixJob.OUT_KEY_TYPE, "int");
    
    mapDriver.setConfiguration(conf);
    mapDriver.withInput(new LongWritable(), new Text("2,3,4.3"));
    List<Pair<WritableComparable, VectorWritable>> outputs = mapDriver.run();
    
    assertTrue(outputs.get(0).getFirst().getClass().equals(IntWritable.class));
    assertTrue(((IntWritable)outputs.get(0).getFirst()).get() == 2);
    assertTrue(outputs.get(0).getSecond().get().isSequentialAccess() == false);
    assertTrue(Math.abs(outputs.get(0).getSecond().get().get(3) - 4.3) < EPSILON);
  }
  
  @SuppressWarnings("rawtypes")
  @Test
  public void testMapperConfig() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(Text2DistributedRowMatrixJob.ROW_IDX_KEY, 1);
    conf.setInt(Text2DistributedRowMatrixJob.COL_IDX_KEY, 0);
    conf.setInt(Text2DistributedRowMatrixJob.VALUE_IDX_KEY, 2);
    conf.setBoolean(Text2DistributedRowMatrixJob.SEQUENTIAL, true);
    conf.set(Text2DistributedRowMatrixJob.OUT_KEY_TYPE, "text");
    
    mapDriver.setConfiguration(conf);
    mapDriver.withInput(new LongWritable(), new Text("2,3,4.3"));
    List<Pair<WritableComparable, VectorWritable>> outputs = mapDriver.run();
    
    assertTrue(outputs.get(0).getFirst().getClass().equals(Text.class));
    assertTrue(((Text)outputs.get(0).getFirst()).toString().equals("3"));
    assertTrue(outputs.get(0).getSecond().get().isSequentialAccess());
    assertTrue(Math.abs(outputs.get(0).getSecond().get().get(2) - 4.3) < EPSILON);
  }
  
  @SuppressWarnings("rawtypes")
  @Test
  public void testReducer() throws IOException {
    List<VectorWritable> vectors = new ArrayList<VectorWritable>();
    Vector v1 = new RandomAccessSparseVector(Integer.MAX_VALUE);
    v1.set(0, 1.0); v1.set(1, 1.0);
    Vector v2 = new RandomAccessSparseVector(Integer.MAX_VALUE);
    v2.set(2, 1.0); v2.set(3, 1.0);
    vectors.add(new VectorWritable(v1));
    vectors.add(new VectorWritable(v2));
    reduceDriver.withInput(new IntWritable(1), vectors);
    List<Pair<WritableComparable, VectorWritable>> outputs = reduceDriver.run();
    
    assertTrue(outputs.get(0).getFirst().getClass().equals(IntWritable.class));
    assertTrue(((IntWritable)outputs.get(0).getFirst()).get() == 1);
    Vector outputVector = outputs.get(0).getSecond().get();
    assertTrue(outputVector.getNumNondefaultElements() == 4);
    Iterator<Vector.Element> cols = outputVector.iterateNonZero();
    while (cols.hasNext()) {
      Vector.Element e = cols.next();
      assertTrue(Math.abs(e.get() - 1.0) < EPSILON);
    }
  }
}
