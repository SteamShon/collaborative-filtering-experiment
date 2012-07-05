package com.skp.experiment.cf.math.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.hadoop.MatrixMultiplicationJob;

import com.skp.experiment.common.HadoopClusterUtil;

public class MatrixMultiplyWithThresholdJob extends
    MatrixMultiplicationJob {
  private static final String OUT_CARD = "output.vector.cardinality";
  private static String THRESHOLD_KEY = "matrix.multiply.threshold";
  
  public static JobConf createMatrixMultiplyWithThresholdJob(Configuration initialConf, 
      Path aPath, 
      Path bPath, 
      Path outPath, 
      int outCardinality, 
      float threshold) throws IOException {
    JobConf conf = new JobConf(initialConf, MatrixMultiplyWithThresholdJob.class);
    conf.setJobName("Matrix Multiply With Threshold Job");
    conf.setInputFormat(CompositeInputFormat.class);
    
    conf.set("mapred.join.expr", CompositeInputFormat.compose(
        "inner", SequenceFileInputFormat.class, new Path(aPath, "part*"), new Path(bPath, "part*")));
    conf.setInt(OUT_CARD, outCardinality);
    conf.setFloat(THRESHOLD_KEY, threshold);
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setInt("io.sort.factor", 100);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    FileOutputFormat.setOutputPath(conf, outPath);
    conf.setMapperClass(MatrixMultiplyMapper.class);
    conf.setCombinerClass(MatrixMultiplicationReducer.class);
    conf.setReducerClass(MatrixMultiplyWithThredsholdReducer.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(VectorWritable.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(VectorWritable.class);
    return conf;
  }
  
  public static class MatrixMultiplyWithThredsholdReducer extends MapReduceBase 
    implements Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {
    private static float threshold = 0;
    private static int outCardinality;
    @Override
    public void configure(JobConf conf) {
      threshold = conf.getFloat(THRESHOLD_KEY, 0);
      outCardinality = conf.getInt(OUT_CARD, Integer.MAX_VALUE);
    }

    @Override
    public void reduce(IntWritable rowNum, Iterator<VectorWritable> it,
        OutputCollector<IntWritable, VectorWritable> out, Reporter reporter)
        throws IOException {
      if (!it.hasNext()) {
        return;
      }
      Vector accumulator = new RandomAccessSparseVector(it.next().get());
      while (it.hasNext()) {
        Vector row = it.next().get();
        accumulator.assign(row, Functions.PLUS);
      }
      Vector prunedOutVector = new RandomAccessSparseVector(outCardinality, 10);
      Iterator<Vector.Element> iter = accumulator.iterateNonZero();
      while (iter.hasNext()) {
        Vector.Element e = iter.next();
        if (e.get() < threshold) {
          continue;
        }
        prunedOutVector.set(e.index(), e.get());
      }
      out.collect(rowNum, new VectorWritable(prunedOutVector));
    }
    
  }
 
}
