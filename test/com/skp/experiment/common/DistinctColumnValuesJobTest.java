package com.skp.experiment.common;

import java.io.File;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.skp.experiment.cf.als.hadoop.ALSMatrixUtil;
import com.skp.experiment.cf.evaluate.hadoop.EvaluatorUtil;
import com.skp.experiment.common.DistinctColumnValuesJob.DistinctColumnValuesMapper;
import com.skp.experiment.common.DistinctColumnValuesJob.DistinctColumnValuesReducer;

public class DistinctColumnValuesJobTest extends MahoutTestCase {
  private File inputFile;
  private File outputDir;
  
  DistinctColumnValuesMapper mapper = new DistinctColumnValuesMapper();
  DistinctColumnValuesReducer reducer = new DistinctColumnValuesReducer();
  
  MapDriver<LongWritable, Text, Text, NullWritable> mapDriver;
  ReduceDriver<Text, NullWritable, NullWritable, Text> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, NullWritable, NullWritable, Text> mapReduceDriver;
  
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    mapper = new DistinctColumnValuesMapper();
    reducer = new DistinctColumnValuesReducer();
    mapDriver = new MapDriver<LongWritable, Text, Text, NullWritable>();
    mapDriver.setMapper(mapper);
    reduceDriver = new ReduceDriver<Text, NullWritable, NullWritable, Text>();
    reduceDriver.setReducer(reducer);
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, NullWritable, NullWritable, Text>(mapper, reducer);
    
    inputFile = getTestTempFile("distinct_column_values_job_input.txt");
    outputDir = getTestTempDir("distinct_column_values_job_ouput");
    outputDir.delete();
  }
  
  @Test
  public void testMapper() {
    String testString = "A,1,0.3,30,XY";
    mapDriver.getConfiguration().set(DistinctColumnValuesJob.COLUMN_INDEXS, "0,1,5");
    mapDriver.withInput(new LongWritable(), new Text(testString));
    mapDriver.withOutput(new Text("A" + DistinctColumnValuesJob.DELIMETER + 0), NullWritable.get());
    mapDriver.withOutput(new Text("1" + DistinctColumnValuesJob.DELIMETER + 1), NullWritable.get());
    mapDriver.runTest();
  }
  
  @Test
  public void testJob() throws Exception {
    Configuration conf = new Configuration();
    String testString = "A,1,0.3,30,XY\nB,2,0.6,23,Z";
    writeLines(inputFile, testString);
    DistinctColumnValuesJob job = new DistinctColumnValuesJob();
    job.setConf(conf);
    job.run(new String[] {
        "--input", inputFile.toString(), "--output", outputDir.toString(), 
        "--columnIndexs", "0,1,5"
    });
    
    Map<String, String> lines = 
        ALSMatrixUtil.fetchTextFiles(new Path(outputDir + "/0"), ",", Arrays.asList(0), Arrays.asList(0));
    assertTrue(lines.size() == 2);
    assertTrue(lines.containsKey("A"));
    assertTrue(lines.containsKey("B"));
    
    lines = 
        ALSMatrixUtil.fetchTextFiles(new Path(outputDir + "/1"), ",", Arrays.asList(0), Arrays.asList(0));
    assertTrue(lines.size() == 2);
    assertTrue(lines.containsKey("1"));
    assertTrue(lines.containsKey("2"));
    
  }
  
}
