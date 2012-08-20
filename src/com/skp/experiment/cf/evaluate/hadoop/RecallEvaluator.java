package com.skp.experiment.cf.evaluate.hadoop;

import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.common.Pair;

public class RecallEvaluator implements Evaluator {
  public RecallEvaluator() {};
  
  public List<Pair<Integer, Double>> evaluate(List<Pair<String, Double>> items,
      int topK, int itemCount, double negativePref) {
    List<Pair<Integer, Double>> result = new ArrayList<Pair<Integer, Double>>();
    int prefCount = 0;
    int sz = Math.min(items.size(), topK);
    int step = topK / 10;
    for (int i = 0; i < sz; i += step) {
      // get hit count from prev ~ next
      for (int j = i; j < sz && j < i + step; j++) {
        Pair<String, Double> item = items.get(j);
        double realRate = item.getSecond();
        if (realRate > negativePref) {
          prefCount++;
        }
      }
      // check truncated recall at this position
      result.add(new Pair<Integer, Double>(prefCount, prefCount / (double)itemCount));
    }
    return result;
  }
  
}
