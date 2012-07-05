package com.skp.experiment.cf.math.hadoop.similarity;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterator;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
/*
 * assumes input vector is in one file
 */
public class MatrixRowVectorSimilarityMapper extends
    Mapper<IntWritable, VectorWritable, NullWritable, VectorWritable> {
  private DistanceMeasure measure;
  private Vector inputVector;
  
  @Override
  protected void map(IntWritable key, VectorWritable value, Context context) 
      throws IOException,
      InterruptedException {
    double distance = measure.distance(value.get(), inputVector);
    Vector outVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
    outVector.set(key.get(), distance);
    context.write(NullWritable.get(), new VectorWritable(outVector));
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    measure = 
        ClassUtils.instantiateAs(
            conf.get(MatrixRowVectorSimilarityJob.DISTANCE_MEASIRE_KEY), 
            DistanceMeasure.class);
    measure.configure(conf);
    inputVector = retrieveInputVector(context);
  }
  
  private Vector retrieveInputVector(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    Path inputVector = new Path(conf.get(MatrixRowVectorSimilarityJob.INPUT_VECTOR_PATH));
    SequenceFileValueIterator<VectorWritable> iterator = 
        new SequenceFileValueIterator<VectorWritable>(inputVector, true, conf);
    return iterator.next().get();
  }
  
}
