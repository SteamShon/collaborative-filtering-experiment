package com.skp.experiment.cf.creditcf.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.mapreduce.VectorSumReducer;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;

import com.skp.experiment.cf.math.hadoop.MatrixRowAggregateJob;
import com.skp.experiment.cf.math.hadoop.similarity.MatrixRowVectorSimilarityJob;

public class UserCreditJob extends AbstractJob {
	private static DistributedRowMatrix R;
	private static DistributedRowMatrix Rtranspose;
	
	
	private static int numRows;
	private static int numCols;
	
	@Override
	public int run(String[] arg0) throws Exception {
		/* MR1. create R */
	  Job userRatingsJob = prepareJob(getInputPath(), pathToUserRatings(),
        TextInputFormat.class, UserRatingVectorsMapper.class, IntWritable.class,
        VectorWritable.class, VectorSumReducer.class, IntWritable.class,
        VectorWritable.class, SequenceFileOutputFormat.class);
    userRatingsJob.setCombinerClass(VectorSumReducer.class);
    userRatingsJob.waitForCompletion(true);
    
    R = new DistributedRowMatrix(pathToUserRatings(), getTempPath(), numRows, numCols, false);
    
    /* MR2. create Rtranspose */
    Rtranspose = R.transpose();
    
    /* MR3. average ratings */
    Job avgRatingsJob = MatrixRowAggregateJob.createAggregateRowJob(
        pathToItemRatings(), pathToAvgRatings());
    avgRatingsJob.waitForCompletion(true);
    
    /* MR4. calculate user credit(similarity)*/
    Job userCreditsJob = 
        MatrixRowVectorSimilarityJob.createMatrixRowVectorSimilarityJob(new Configuration(), 
            pathToUserRatings(), pathToAvgRatings(), pathToUserCredits(), 
            CosineDistanceMeasure.class.getName());
    userCreditsJob.waitForCompletion(true);
    
	  return 0;
	}
	
	/*
	 * return transposed matrix
	 */
	protected Path pathToItemRatings() {
	  return new Path("");
	}
	protected Path pathToUserRatings() {
	  return new Path("");
	}
	protected Path pathToAvgRatings() {
	  return new Path("");
	}
	protected Path pathToUserCredits() {
	  return new Path("");
	}
	protected Path pathToUserCreditsTemp() {
	  return new Path("");
	}
	
}
