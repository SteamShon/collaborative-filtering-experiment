package com.skp.experiment.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

public class OptionParseUtil {
  public static final String DELIMETER = ",";
  public static List<Integer> decode(String s, String delimeter) {
    List<Integer> idxs = new ArrayList<Integer>();
    String[] tokens = s.split(delimeter);
    for (int i = 0; i < tokens.length; i++) {
      if (tokens[i].equals(DELIMETER) || tokens[i].equals("")) continue;
      try {
        idxs.add(Integer.parseInt(tokens[i]));
      } catch (Exception e) {
        continue;
      }
    }
    return idxs;
  }
  
  public static String encode(String[] args, List<Integer> idxs, String delimeter) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < idxs.size(); i++) {
      int idx = idxs.get(i);
      if (idx < 0 || idx >= args.length) {
        continue;
      }
      if (i != 0) sb.append(delimeter);
      sb.append(args[idx]);
    }
    return sb.toString();
  }
  
  public static String convertToString(List<Integer> list, String delimeter) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < list.size(); i++) {
      if (i != 0) {
        sb.append(delimeter);
      }
      sb.append(list.get(i));
    }
    return sb.toString();
  }
  
  public static String getAttemptId(Configuration conf) throws IllegalArgumentException
  {
      if (conf == null) {
          throw new NullPointerException("conf is null");
      }

      String taskId = conf.get("mapred.task.id");
      if (taskId == null) {
          throw new IllegalArgumentException("Configutaion does not contain the property mapred.task.id");
      }

      String[] parts = taskId.split("_");
      if (parts.length != 6 ||
          !parts[0].equals("attempt") ||
          (!"m".equals(parts[3]) && !"r".equals(parts[3]))) {
        throw new IllegalArgumentException("TaskAttemptId string : " + taskId + " is not properly formed");
      }
      
      return "part-" + parts[3] + "-" + parts[4];
  }
  
}
