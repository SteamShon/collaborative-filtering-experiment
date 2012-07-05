package com.skp.experiment.cf.evaluate.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.iterator.FileLineIterator;

import com.skp.experiment.common.OptionParseUtil;

public class EvaluatorUtil {
  public static Integer RECORD_COUNT_SUM_INDEX = -1;
  public static final String NEWLINE = System.getProperty("line.separator");
  
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
  public static Map<String, String> fetchTextFiles(Path input, String delimeter,
      List<Integer> keyIdxs, List<Integer> valueIdxs) throws IOException {
    Configuration conf = new Configuration();
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
        if (valueIdxs.size() == 0) {
          for (int i = 0; i < tokens.length; i++) {
            if (!keyIdxs.contains(i)) {
              valueIdxs.add(i);
            }
          }
        }
        String value = OptionParseUtil.encode(tokens, valueIdxs, delimeter);
        caches.put(key, value);
      }
    }
    return caches;
  }
  public static void deletePartFiles(Configuration conf, Path dir) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.globStatus(new Path(dir.toString() + "/part*"));
    for (FileStatus file : files) {
      fs.delete(file.getPath(), true);
    }
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
  public static Map<String, String> fetchTextFile(Path input, String delimeter,
      List<Integer> keyIdxs, List<Integer> valueIdxs) throws IOException {
    Configuration conf = new Configuration();
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
    }
    return caches;
  }

  public static Map<String, Integer> fetchTextFilesWithIntegerValue(Context ctx, Path input, String delimeter,
      List<Integer> keyIdxs, List<Integer> valueIdxs) throws IOException {
    Configuration conf = ctx.getConfiguration();
    Map<String, Integer> caches = new HashMap<String, Integer>();
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
        caches.put(key, Integer.parseInt(value));
        if (caches.size() % 1000000 == 0) {
          ctx.setStatus("fetched " + caches.size());
        }
      }
    }
    return caches;
  }

  /** 
   * iterate files under output path and sums over each column indexs value as double .
   * number of record count is will be on -1 column
   * @param conf
   * @param output
   * @param indexs
   * @return
   * @throws IOException
   */
  public static Map<Integer, Double> getResultSumPerColumns(Configuration conf, Path output, List<Integer> indexs, boolean exclude) 
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.globStatus(new Path(output.toString() + "/^[^_]*"));
    Map<Integer, Double> stats = new HashMap<Integer, Double>();
    long totalCount = 0;
    for (FileStatus file : files) {
      FileLineIterator iter = new FileLineIterator(fs.open(file.getPath()));
      while (iter.hasNext()) {
        totalCount++;
        String[] tokens = TasteHadoopUtils.splitPrefTokens(iter.next());
        if (exclude) {
          for (int idx = 0; idx < tokens.length; idx++) {
            if (indexs.contains(idx)) continue;
            if (!stats.containsKey(idx)) {
              stats.put(idx, 0.0);
            }
            stats.put(idx, stats.get(idx) + Double.parseDouble(tokens[idx]));
          }
        } else {
          for (Integer idx : indexs) {
            if (idx >= 0 && idx < tokens.length) {
              if (!stats.containsKey(idx)) {
                stats.put(idx, 0.0);
              }
              stats.put(idx, stats.get(idx) + Double.parseDouble(tokens[idx]));
            }
          }
        }
      }
    }
    stats.put(RECORD_COUNT_SUM_INDEX, (double) totalCount);
    return stats;
  }
  /**
   * write string into hdfs file. new line is padded
   * @param conf
   * @param output
   * @param outputString
   * @throws IOException
   */
  public static void writeToHdfs(Configuration conf, Path output, String outputString) 
      throws IOException {
    writeToHdfs(conf, output, outputString, true);
  }
  /**
   * 
   * @param conf
   * @param output
   * @param outputString
   * @param newline
   * @throws IOException
   */
  public static void writeToHdfs(Configuration conf, Path output, String outputString, boolean newline) 
      throws IOException {
    FSDataOutputStream out = null;
    try {
      FileSystem fs = FileSystem.get(conf);
      out = fs.create(fs.makeQualified(output));
      if (newline) {
        out.writeBytes(outputString + NEWLINE);
      } else {
        out.writeBytes(outputString);
      }
    } finally {
      if (out != null) {
        out.close();
      }
    } 
  }
  public static void renamePath(Configuration conf, Path input, Path output) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    fs.rename(input, output);
  }
}
