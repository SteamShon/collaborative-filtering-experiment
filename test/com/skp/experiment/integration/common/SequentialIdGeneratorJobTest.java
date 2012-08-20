package com.skp.experiment.integration.common;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.junit.Before;
import org.junit.Test;

import com.skp.experiment.cf.als.hadoop.ALSMatrixUtil;
import com.skp.experiment.cf.evaluate.hadoop.EvaluatorUtil;
import com.skp.experiment.common.MahoutTestCase;
import com.skp.experiment.integeration.common.SequentialIdGeneratorJob;
import com.skp.experiment.integeration.common.SequentialIdGeneratorJob.CountPartitionRecordNumMapper;
import com.skp.experiment.integeration.common.SequentialIdGeneratorJob.CountPartitionRecordNumReducer;

public class SequentialIdGeneratorJobTest extends MahoutTestCase {
  private File inputFile;
  private File outputDir;
  private File tempDir;
  private Map<String, String> expected;
  
  MapDriver<LongWritable, Text, IntWritable, LongWritable> mapDriver;
  ReduceDriver<IntWritable, LongWritable, IntWritable, LongWritable> reduceDriver;
  CountPartitionRecordNumMapper mapper = new CountPartitionRecordNumMapper();
  CountPartitionRecordNumReducer reducer = new CountPartitionRecordNumReducer();
  
  
  
  @Before
  public void setUp() throws IOException {
    mapDriver = new MapDriver<LongWritable, Text, IntWritable, LongWritable>();
    reduceDriver = new ReduceDriver<IntWritable, LongWritable, IntWritable, LongWritable>();
    mapDriver.setMapper(mapper);
    reduceDriver.setReducer(reducer);
    inputFile = getTestTempFile("sequential_id_input.txt");
    outputDir = getTestTempDir("sequential_id_output");
    tempDir = getTestTempDir("sequential_id_tmp");
    outputDir.delete();
    tempDir.delete();
  }
  private void writeTestCase(File testFile, int startIndex) throws IOException {
    expected = new HashMap<String, String>();
    StringBuffer testStr = new StringBuffer();
    int testSize = 10;
    int offset = 1000;
    for (int i = 0; i < testSize; i++) {
      if (i != 0) {
        testStr.append("\n");
      }
      testStr.append(i + offset);
      expected.put(String.valueOf(i + startIndex), String.valueOf(i + offset));
    }
    writeLines(testFile, testStr.toString());
  }
  
  @Test
  public void testCountPartitionRecordNumMapper() throws IOException {
    int partitionId = 11111;
    File recordPath = getTestTempDir("records");
    Configuration conf = new Configuration();
    conf.setInt("mapred.task.partition", partitionId);
    conf.set(SequentialIdGeneratorJob.RECORDS_PATH, recordPath.toString());
    mapDriver.withConfiguration(conf);
    mapDriver.withInput(new LongWritable(), new Text("line"));
    mapDriver.withOutput(new IntWritable(partitionId), new LongWritable(1));
    mapDriver.runTest();
    mapDriver.run();
    Path mapOutputPath = new Path(recordPath + "/" + String.format("records%05d", partitionId));
    SequenceFileIterator<Text, Text> iter = 
        new SequenceFileIterator<Text, Text>(mapOutputPath, true, conf);
    Pair<Text, Text> pair = iter.next();
    System.out.println(pair.toString());
    assertTrue(pair.getFirst().toString().equals(partitionId + "," + 0));
    assertTrue(pair.getSecond().toString().equals("line"));
  }
  
  private void runTestWithStartIndex(int startIndex) throws Exception {
    inputFile = getTestTempFile("sequential_id_input" + startIndex);
    outputDir = getTestTempDir("sequential_id_output" + startIndex);
    tempDir = getTestTempDir("sequential_id_tmp" + startIndex);
    inputFile.delete();
    outputDir.delete();
    tempDir.delete();
    writeTestCase(inputFile, startIndex);
    
    SequentialIdGeneratorJob job = new SequentialIdGeneratorJob();
    job.setConf(new Configuration());
    job.run(new String[]{
        "-i", inputFile.toString(), "-o", outputDir.toString(),
        "--cleanUp", "true", "--startIndex", String.valueOf(startIndex), "--tempDir", tempDir.toString()
    });
    Map<String, String> lines = 
        ALSMatrixUtil.fetchTextFiles(new Path(outputDir.toString()), ",", Arrays.asList(0), Arrays.asList(1));
    
    for (Entry<String, String> line : lines.entrySet()) {
      assertTrue(expected.containsKey(line.getKey()));
      assertTrue(expected.get(line.getKey()).equals(line.getValue()));
    }
  }
  /**
   * test sequential id generate job with various startIndex
   */
  @Test
  public void testJob() throws Exception {
    int[] startIndexs = new int[]{0, 1, 100, 923};
    for (int i = 0; i < startIndexs.length; i++) {
      runTestWithStartIndex(startIndexs[i]);
    }
  }
}
