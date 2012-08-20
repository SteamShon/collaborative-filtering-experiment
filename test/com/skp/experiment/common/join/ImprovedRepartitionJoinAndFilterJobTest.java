package com.skp.experiment.common.join;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
import com.skp.experiment.common.MahoutTestCase;
import com.skp.experiment.common.join.ImprovedRepartitionJoinAndFilterJob.ImprovedRepartitionJoinMapper;
import com.skp.experiment.common.join.ImprovedRepartitionJoinAndFilterJob.ImprovedRepartitionJoinReducer;

public class ImprovedRepartitionJoinAndFilterJobTest extends MahoutTestCase {
  private File inputFile;
  private File tgtFile;
  private File outputDir;
  
  ImprovedRepartitionJoinMapper mapper = new ImprovedRepartitionJoinMapper();
  ImprovedRepartitionJoinReducer reducer = new ImprovedRepartitionJoinReducer();

  MapDriver<LongWritable, Text, CompositeJoinKey, CompositeJoinValue> mapDriver;
  ReduceDriver<CompositeJoinKey, CompositeJoinValue, NullWritable, Text> reduceDriver;
  MapReduceDriver<LongWritable, Text, CompositeJoinKey, CompositeJoinValue, NullWritable, Text> mapReduceDriver;

  Map<String, Map<String, Boolean>> expected;
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    mapDriver = new MapDriver<LongWritable, Text, CompositeJoinKey, CompositeJoinValue>();
    mapDriver.setMapper(mapper);
    reduceDriver = new ReduceDriver<CompositeJoinKey, CompositeJoinValue, NullWritable, Text>();
    reduceDriver.setReducer(reducer);
    inputFile = getTestTempFile("join_input.txt");
    tgtFile = getTestTempFile("join_tgt_input.txt");
    
    initTestData();
    expectedOutput();
  }
  protected void initTestData() throws IOException {
    String testString = 
        "A,I1,0.43,-1,4\n" + 
            "A,I2,0.32,0,4\n" + 
            "A,I3,0.23,0,4\n" + 
            "B,I2,0.32,0,2\n" +
            "B,I4,0.21,0,2\n" + 
            "B,I5,0.12,-1,2\n" + 
            "B,I6,0.11,-1,2\n" + 
            "B,I7,0.09,-1,2\n" + 
            "C,I2,0.32,0,3\n" + 
            "C,I4,0.21,0,3\n" + 
            "C,I5,0.12,-1,3\n" + 
            "C,I6,0.11,2,3\n" +
            "C,I7,0.09,-1,3";
    String tgtString = 
        "A,I1\n" + 
            "B,I3\n" +
            "B,I4\n" +
            "A,I2\n" + 
            "C,I2\n";
    writeLines(inputFile, testString);
    writeLines(tgtFile, tgtString);
  }
  protected void expectedOutput() {
    expected = new HashMap<String, Map<String, Boolean>>();
    expected.put("inner", new HashMap<String, Boolean>());
    expected.put("outer", new HashMap<String, Boolean>());
    expected.put("filter", new HashMap<String, Boolean>());
    
    expected.get("inner").put("A,I1,0.43,-1,4,A", true);
    expected.get("inner").put("B,I4,0.21,0,2,B", true);
    expected.get("inner").put("A,I2,0.32,0,4,A", true);
    expected.get("inner").put("C,I2,0.32,0,3,C", true);
    
    expected.get("outer").put("A,I1,0.43,-1,4,A", true);
    expected.get("outer").put("A,I2,0.32,0,4,A", true);
    expected.get("outer").put("A,I3,0.23,0,4,null", true);
    expected.get("outer").put("B,I2,0.32,0,2,null", true);
    expected.get("outer").put("B,I4,0.21,0,2,B", true);
    expected.get("outer").put("B,I5,0.12,-1,2,null", true);
    expected.get("outer").put("B,I6,0.11,-1,2,null", true);
    expected.get("outer").put("B,I7,0.09,-1,2,null", true);
    expected.get("outer").put("C,I2,0.32,0,3,C", true);
    expected.get("outer").put("C,I4,0.21,0,3,null", true);
    expected.get("outer").put("C,I5,0.12,-1,3,null", true);
    expected.get("outer").put("C,I6,0.11,2,3,null", true);
    expected.get("outer").put("C,I7,0.09,-1,3,null", true);
    
    expected.get("filter").put("A,I3,0.23,0,4", true);
    expected.get("filter").put("B,I2,0.32,0,2", true);
    expected.get("filter").put("B,I5,0.12,-1,2", true);
    expected.get("filter").put("B,I6,0.11,-1,2", true);
    expected.get("filter").put("B,I7,0.09,-1,2", true);
    expected.get("filter").put("C,I4,0.21,0,3", true);
    expected.get("filter").put("C,I5,0.12,-1,3", true);
    expected.get("filter").put("C,I6,0.11,2,3", true);
    expected.get("filter").put("C,I7,0.09,-1,3", true);
    
  }
  
  @Test
  public void ImprovedRepartitionJoinMapperTest() throws Exception {
    String[] joinTypes = new String[]{"inner", "filter", "outer"}; 
    String[] mapOnlyOptions = new String[] {"true", "false"};
    for (int j = 0; j < mapOnlyOptions.length; j++) {
      for (int i = 0; i < joinTypes.length; i++) {
        outputDir = getTestTempDir(joinTypes[i] + "_" + mapOnlyOptions[j]);
        outputDir.delete();
        ImprovedRepartitionJoinAndFilterJob job = new ImprovedRepartitionJoinAndFilterJob();
        job.setConf(new Configuration());
        job.run(new String[]{
            "--input", inputFile.toString(), "--output", outputDir.toString(), 
            "--srcKeyIndex", "0,1", "--tgtTableOptions", 
            tgtFile.toString() + ":0,1:0,1:0:" + joinTypes[i], "--defaultValue", "null",
            "--mapOnly", mapOnlyOptions[j]
        });
        
        Map<String, String> lines = 
            ALSMatrixUtil.fetchTextFiles(new Path(outputDir.toString()), ",", Arrays.asList(0,1,2,3,4,5), Arrays.asList(0));
        assertTrue(expected.get(joinTypes[i]).size() == lines.size());
       
        for (Entry<String, String> line : lines.entrySet()) {
          assertTrue(expected.get(joinTypes[i]).containsKey(line.getKey()));
        }
      }
    }
  }
 
}
