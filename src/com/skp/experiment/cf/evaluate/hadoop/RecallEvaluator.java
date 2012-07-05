package com.skp.experiment.cf.evaluate.hadoop;

import java.util.List;

import org.apache.mahout.common.Pair;

public class RecallEvaluator implements Evaluator {
  public RecallEvaluator() {};
  @Override
  public Pair<Integer, Double> evaluate(List<Pair<String, Double>> items,
      int topK, int itemCount, double negativePref) {
    int prefCount = 0;
    int sz = Math.min(items.size(), topK);
    for (int i = 0; i < sz; i++) {
      Pair<String, Double> item  = items.get(i);
      double realRate = item.getSecond();
      if (realRate > negativePref) {
        prefCount++;
      }
    }
    return new Pair<Integer, Double>(prefCount, prefCount / (double)itemCount);
  }

}
