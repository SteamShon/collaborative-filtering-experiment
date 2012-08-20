package com.skp.experiment.cf.als.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.skp.experiment.cf.evaluate.hadoop.EvaluatorUtil;
import com.skp.experiment.common.MahoutTestCase;
/**
 * This test cases is for integration test on Conv2IndexJob
 * Conv2IndexJob consists of multiple jobs that has been already unit-tested.
 * @author doyoungYoon
 *
 */
public class Conv2IndexJobTest extends MahoutTestCase {
  private File inputFile;
  private File outputDir;
  private File tempDir;
  private int testSize = 10;
  @Before
  public void setUp() throws IOException {
    inputFile = getTestTempFile("conv2IndexJob_in");
    outputDir = getTestTempDir("conv2IndexJob_out");
    tempDir = getTestTempDir("conv2IndexJob_tmp");
    
  }
  private void writeTestCase(File testFile) throws IOException {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < testSize; i++) {
      if (i != 0) {
        sb.append("\n");
      }
      sb.append((char)('a' + i)).append(",").append((char)('A' + i));
    }
    writeLines(testFile, sb.toString());
  }
  @Test
  public void testMapsideJob() throws Exception {
    writeTestCase(inputFile);
    outputDir.delete();
    
    
    Conv2IndexJob job = new Conv2IndexJob();
    job.setConf(new Configuration());
    job.run(new String[]{
        "-i", inputFile.toString(), "-o", outputDir.toString(),
        "--columnIndexs", "0,1", "--mapOnlyColumnIndexs", "0,1",
        "--tempDir", tempDir.toString()
    });
    testColumnIndex(0);
    testColumnIndex(1);
    testResult();
  }
  
  //check column index
  private void testColumnIndex(int index) throws IOException {
    Path indexOutput = new Path(outputDir.toString() + "_index_new");
    Map<String, String> lines = 
        ALSMatrixUtil.fetchTextFiles(new Path(indexOutput + "/" + index), ",", Arrays.asList(0), Arrays.asList(1));
    assertTrue(lines.size() == testSize);
    for (int i = 0; i < testSize; i++) {
      char targetChar = index == 0 ? 'a' : 'A';
      String expectedValue = String.valueOf((char)(targetChar + i));
      assertTrue(lines.containsKey(String.valueOf(i)));
      assertTrue(lines.get(String.valueOf(i)).equals(expectedValue));
    }
  }
 
  // check output
  private void testResult() throws IOException {
    Map<String, String> lines = 
        ALSMatrixUtil.fetchTextFiles(new Path(outputDir.toString()), ",", Arrays.asList(0), Arrays.asList(1));
    assertTrue(lines.size() == testSize);
    for (int i = 0; i < testSize; i++) {
      assertTrue(lines.containsKey(String.valueOf(i)));
      assertTrue(lines.get(String.valueOf(i)).equals(String.valueOf(i)));
    }
  }
}
