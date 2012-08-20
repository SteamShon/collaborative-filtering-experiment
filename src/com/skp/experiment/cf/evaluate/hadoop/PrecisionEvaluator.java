package com.skp.experiment.cf.evaluate.hadoop;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Pair;

public class PrecisionEvaluator implements Evaluator {
  public PrecisionEvaluator() {};
  
  @Override
  public List<Pair<Integer, Double>> evaluate(List<Pair<String, Double>> items,
      int topK, int itemCount, double negativePref) {
    int prefCount = 0;
    double precision = 0.0;
    int sz = Math.min(items.size(), topK);
    for (int i = 0; i < sz; i++) {
      Pair<String, Double> item  = items.get(i);
      double realRate = item.getSecond();
      if (realRate > negativePref) {
        prefCount++;
      }
    }
    precision = (double)prefCount / topK;
    return Arrays.asList(new Pair<Integer, Double>(prefCount, precision));
  }
}
