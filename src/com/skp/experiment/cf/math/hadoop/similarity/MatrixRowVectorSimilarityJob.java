package com.skp.experiment.cf.math.hadoop.similarity;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterator;
import org.apache.mahout.common.mapreduce.MergeVectorsCombiner;
import org.apache.mahout.common.mapreduce.MergeVectorsReducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class MatrixRowVectorSimilarityJob extends AbstractJob {

  //public static final String INPUT_VECTOR_OUTPUT_PATH = "DistributedMatrix.rowsimilarity";
  public static final String INPUT_VECTOR_PATH = "";
  public static final String DISTANCE_MEASIRE_KEY = "distance.measure";
  /*
   * input: matrix input, input vector, distance measure
   */
  @Override 
  public int run(String[] args) throws Exception {
    /* setup input options */
    addInputOption();
    addOutputOption();
    addOption(DefaultOptionCreator.distanceMeasureOption().create());
    addOption(INPUT_VECTOR_PATH, "iv", "input vector path.");
    if (parseArguments(args) == null) {
      return -1;
    }
    Path input = getInputPath();
    Path output = getOutputPath();
    Path inputVector = new Path(getOption(INPUT_VECTOR_PATH));
    /* setup distance measure class */
    String measureClass = getOption(DefaultOptionCreator.DISTANCE_MEASURE_OPTION);
    if (measureClass == null) {
      measureClass = CosineDistanceMeasure.class.getName();
    }
    DistanceMeasure measure = ClassUtils.instantiateAs(measureClass, DistanceMeasure.class);
    if (getConf() == null) {
      setConf(new Configuration());
    }
    /* set required parameters into configuration */
    Configuration conf = getConf();
    conf.set(DISTANCE_MEASIRE_KEY, measure.getClass().getName());
    conf.set(INPUT_VECTOR_PATH, inputVector.getName());
    /* build job */
    Job job = prepareJob(input, output, SequenceFileInputFormat.class,
        MatrixRowVectorSimilarityMapper.class, NullWritable.class, VectorWritable.class,
        MergeVectorsReducer.class, NullWritable.class, VectorWritable.class,
        SequenceFileOutputFormat.class);
    job.setJarByClass(MatrixRowVectorSimilarityJob.class);
    job.setCombinerClass(MergeVectorsCombiner.class);
    job.waitForCompletion(true);
    return 0;
  }
  /*
   * matrixPath: <IntWritable, VectorWritable>
   * inputVectorPath: <NullWritable, VectorWritable> 
   */
  public static Job createMatrixRowVectorSimilarityJob(Configuration conf, Path matrixPath, 
                                                       Path inputVectorPath, 
                                                       Path outputPath, String measureClass) 
    throws IOException {
    conf.set(DISTANCE_MEASIRE_KEY, measureClass);
    conf.set(INPUT_VECTOR_PATH, inputVectorPath.getName());
    Job job = new Job(conf, "Matrix row vector similarity job");
    job.setJarByClass(MatrixRowVectorSimilarityJob.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job, matrixPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setMapperClass(MatrixRowVectorSimilarityMapper.class);
    job.setReducerClass(MergeVectorsReducer.class);
    job.setCombinerClass(MergeVectorsCombiner.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(VectorWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    return job;
  }
  
  public static Vector retrieveMatrixRowVectorSimilarity(Configuration conf, Path output) throws IOException {
    SequenceFileValueIterator<VectorWritable> iterator = 
        new SequenceFileValueIterator<VectorWritable>(output, true, conf);
    return iterator.next().get();
  }
}
