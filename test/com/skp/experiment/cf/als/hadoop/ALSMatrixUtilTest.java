package com.skp.experiment.cf.als.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.skp.experiment.common.MahoutTestCase;

public class ALSMatrixUtilTest extends MahoutTestCase {
  private File inputFile;
  private File outputDir;
  private File tempDir;
  private int testSize = 10;
  
  @Before
  public void setUp() throws IOException {
    inputFile = getTestTempFile("alsMatrixUtilTestIn");
    outputDir = getTestTempDir("alsMatrixUtilTestOut");
    tempDir = getTestTempDir("alsMatrixUtilTestTmp");
  }
  private void writeTestCase(File testFile) throws IOException {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < testSize; i++) {
      if (i != 0) {
        sb.append("\n");
      }
      sb.append((char)('A' + i)).append(",").append((char)('A' + 32 + i));
      sb.append(",").append((char)('A' - 32 + i));
    }
    writeLines(testFile, sb.toString());
  }
  @Test
  public void testFetchTextFilesWhenPathIsFile() throws IOException {
    writeTestCase(inputFile);
    outputDir.delete();
    Map<String, String> lines = 
        ALSMatrixUtil.fetchTextFiles(new Path(inputFile.toString()), ",", Arrays.asList(0), Arrays.asList(1));
    assertTrue(lines.size() == testSize);
    for (int i = 0; i < testSize; i++) {
        String expKey = String.valueOf((char)('A' + i));
        String expValue = String.valueOf((char)('A' + 32 +i));
        assertTrue(lines.containsKey(expKey));
        assertTrue(lines.get(expKey).equals(expValue));
    }
  }
  
  @Test
  public void testFetchTextFilesWithIndexs() throws IOException {
    writeTestCase(inputFile);
    outputDir.delete();
    Map<String, String> lines = 
        ALSMatrixUtil.fetchTextFiles(new Path(inputFile.toString()), ",", Arrays.asList(0), Arrays.asList(2));
    assertTrue(lines.size() == testSize);
    for (int i = 0; i < testSize; i++) {
        String expKey = String.valueOf((char)('A' + i));
        String expValue = String.valueOf((char)('A' - 32 + i));
        assertTrue(lines.containsKey(expKey));
        assertTrue(lines.get(expKey).equals(expValue));
    }
  }
}
