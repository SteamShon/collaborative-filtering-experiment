package com.skp.experiment.common.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.impl.common.FullRunningAverage;
import org.apache.mahout.cf.taste.impl.common.RunningAverage;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;


public class AverageVectorMapper extends Mapper<IntWritable,VectorWritable,NullWritable,VectorWritable> {
  @Override
  protected void map(IntWritable r, VectorWritable v, Context ctx) throws IOException, InterruptedException {
    RunningAverage avg = new FullRunningAverage();
    Iterator<Vector.Element> elements = v.get().iterateNonZero();
    while (elements.hasNext()) {
      avg.addDatum(elements.next().get());
    }
    Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
    vector.setQuick(r.get(), avg.getAverage());
    ctx.write(NullWritable.get(), new VectorWritable(vector));
  }
}