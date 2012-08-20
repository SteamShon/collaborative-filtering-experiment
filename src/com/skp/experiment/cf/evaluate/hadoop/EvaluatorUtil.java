package com.skp.experiment.cf.evaluate.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.iterator.FileLineIterator;

public class EvaluatorUtil {
  public static Integer RECORD_COUNT_SUM_INDEX = -1;
  
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
  
}
