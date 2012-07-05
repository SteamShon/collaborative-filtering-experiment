package com.skp.experiment.cf.math.hadoop.similarity;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Test;

public class TestMatrixRowVectorSimilarityJob {

  
  public void testMatrixRowVectorSimilarityMapper() {
    double[] arr = {3, 4, 1, 2, 5};
    Vector inputVector = new DenseVector(arr);
    MatrixRowVectorSimilarityMapper mapper = new 
        MatrixRowVectorSimilarityMapper();
    Mapper<IntWritable, VectorWritable, NullWritable, VectorWritable>.Context context = 
        mock(Mapper.Context.class);
    Configuration conf = context.getConfiguration();
  }
}
