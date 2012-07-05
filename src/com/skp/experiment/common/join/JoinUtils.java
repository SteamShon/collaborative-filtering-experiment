package com.skp.experiment.common.join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.skp.experiment.common.OptionParseUtil;

public class JoinUtils {
  public static Map<String, String> fetchTextFiles(Context ctx, Path input, String delimeter,
      List<Integer> keyIdxs, List<Integer> valueIdxs) throws IOException {
    Configuration conf = ctx.getConfiguration();
    Map<String, String> caches = new HashMap<String, String>();
    // read target file.
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.globStatus(new Path(input.toString() + "/^[^_]*"));
    for (FileStatus file : files) {
      FSDataInputStream in = fs.open(file.getPath());
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String line = null;
      while ((line = reader.readLine()) != null) {
        String[] tokens = line.split(delimeter);
        // record target key, value
        String key = OptionParseUtil.encode(tokens, keyIdxs, delimeter);
        String value = OptionParseUtil.encode(tokens, valueIdxs, delimeter);
        caches.put(key, value);
        if (caches.size() % 1000000 == 0) {
          ctx.setStatus("fetched " + caches.size());
        }
      }
    }
    return caches;
  }
  public static Map<String, String> fetchTextFile(Context ctx, Path input, String delimeter,
      List<Integer> keyIdxs, List<Integer> valueIdxs) throws IOException {
    Configuration conf = ctx.getConfiguration();
    Map<String, String> caches = new HashMap<String, String>();
    // read target file.
    FileSystem fs = FileSystem.get(conf);
    FSDataInputStream in = fs.open(input);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String line = null;
    while ((line = reader.readLine()) != null) {
      String[] tokens = line.split(delimeter);
      // record target key, value
      String key = OptionParseUtil.encode(tokens, keyIdxs, delimeter);
      String value = OptionParseUtil.encode(tokens, valueIdxs, delimeter);
      caches.put(key, value);
      if (caches.size() % 1000000 == 0) {
        ctx.setStatus("fetched " + caches.size());
      }
    }
    return caches;
  }
}
