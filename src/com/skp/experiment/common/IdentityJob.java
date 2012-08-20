package com.skp.experiment.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenIntObjectHashMap;

import com.skp.experiment.common.mapreduce.ReferenceMapper;
import com.skp.experiment.common.parameter.DefaultOptionCreator;


public class IdentityJob extends AbstractJob {
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new IdentityJob(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("refer", null, "reference table");
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    //OpenIntObjectHashMap<Vector> testCases = createTestData(new Path(getOption("refer")));
    int i = 0;
    for (String s : getOption("refer").split(DefaultOptionCreator.COMMA_DELIMETER)) {
      i++;
      Map<IntWritable, VectorWritable> testCases = createTestData(new Path(s));
      for (Entry<IntWritable, VectorWritable> v : testCases.entrySet()) {
        System.out.println(i + "\t" + v.getKey() + "\t" + v.getValue());
      }
    }
    
    
    Job job = prepareJob(getInputPath(), getOutputPath("time-" + System.currentTimeMillis()), TextInputFormat.class, 
        MyMapper.class, NullWritable.class, Text.class, TextOutputFormat.class);
    job.getConfiguration().set(ReferenceMapper.REFERENCE_PATHS, getOption("refer"));
    job.waitForCompletion(true);
    return 0;
  }
  private Map<IntWritable, VectorWritable> createTestData(Path referencePath) throws IOException {
    FileSystem fs = FileSystem.get(getConf());
    SequenceFile.Writer writer = null;
    Map<IntWritable, VectorWritable> ret = new HashMap<IntWritable, VectorWritable>();
    try {
      writer = SequenceFile.createWriter(fs, getConf(), referencePath, IntWritable.class, VectorWritable.class);
      Random random = new Random();
      for (int i = 0; i < 10; i++) {
        Vector v = new RandomAccessSparseVector(10);
        for (int j = 0; j < 10; j++) {
          v.set(j, random.nextDouble() * 10);
        }
        ret.put(new IntWritable(i), new VectorWritable(v));
        writer.append(new IntWritable(i), new VectorWritable(v));
      }
    } finally {
      IOUtils.closeStream(writer);
    }
    return ret;
  }
  public static class MyMapper 
    extends ReferenceMapper<LongWritable, Text, NullWritable, Text, IntWritable, VectorWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    }
  }
}
