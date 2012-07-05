package com.skp.experiment.cf.evaluate.hadoop;

import java.util.List;

import org.apache.mahout.common.Pair;

public class ExpectedPercentileRankEvaluator implements Evaluator {
  private static final double EPS = 10e-6;
  public ExpectedPercentileRankEvaluator() {};
  @Override
  public Pair<Integer, Double> evaluate(List<Pair<String, Double>> items,
      int topK, int itemCount, double negativePref) {
    
    double expectedSum = 0.0;
    double realSum = 0.0;
    int prefCount = 0;
    int sz = Math.min(items.size(), topK);
    for (int i = 0; i < sz; i++) {
      Pair<String, Double> item  = items.get(i);
      double realRate = item.getSecond();
      if (realRate > negativePref) {
        prefCount++;
      }
      double percentile = 0.0;
      if (i == sz-1) {
        percentile = 100.0;
      } else {
        percentile = 100.0 * i / items.size();
      }
      realSum += percentile * realRate;
      expectedSum += realRate;
    }
    if (expectedSum < EPS) {
      return new Pair<Integer, Double>(prefCount, 0.0);
    }
    return new Pair<Integer, Double>(prefCount, realSum / expectedSum);
  }
}
