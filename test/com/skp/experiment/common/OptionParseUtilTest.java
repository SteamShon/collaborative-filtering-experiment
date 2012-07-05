package com.skp.experiment.common;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class OptionParseUtilTest {
  private String DELIMETER = ",";
  @Test
  public void testDecode() {
    String[] testStrings = new String[] { 
        "1,2,3",
        ",,,1,2,3,", 
        "1,,2,3,,,",
        "1,......,.....,2,.....,,,3,//n/"};
    for (int i = 0; i < testStrings.length; i++) {
      List<Integer> values = OptionParseUtil.decode(testStrings[i], DELIMETER);
      assertTrue(values.size() == 3);
      assertTrue(values.get(0) == 1);
      assertTrue(values.get(1) == 2);
      assertTrue(values.get(2) == 3);
    }
  }
  @Test
  public void testEncode() {
    List<Integer> indexs = Arrays.asList(0,2,1);
    Map<String, String> result = new HashMap<String, String>();
    result.put("343,12,,434,343", "343,,12");
    result.put("x,324,231,23,123", "x,231,324");
    result.put("x,", "x");
    for (Entry<String, String> item : result.entrySet()) {
      assertTrue(item.getValue().equals(
          OptionParseUtil.encode(item.getKey().split(DELIMETER), indexs, DELIMETER)));
    }
  }
  
  @Test
  public void testGetAttemptId() {
    Configuration conf = new Configuration();
    String taskAttempId = "attempt_201207021707_0527_m_000000_0";
    conf.set("mapred.task.id", taskAttempId);
    assertTrue(OptionParseUtil.getAttemptId(conf).equals("part-m-000000"));
  }
}
