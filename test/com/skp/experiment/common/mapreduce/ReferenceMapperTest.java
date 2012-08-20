package com.skp.experiment.common.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class ReferenceMapperTest {
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  
  @Before
  public void setUp() throws Exception {
    mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
    mapDriver.setMapper(new ReferenceMapper<LongWritable, Text, Text, Text, Text, Text>());
  }
  @Test
  public void test() {
    
  }

}
