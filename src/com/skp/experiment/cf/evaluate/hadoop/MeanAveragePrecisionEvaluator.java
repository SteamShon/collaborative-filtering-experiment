package com.skp.experiment.cf.evaluate.hadoop;

import java.util.List;

import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Pair;



public class MeanAveragePrecisionEvaluator implements Evaluator {
  /*
   * assumes sorted list of <estimated rate, boolean real rate(0/1)>
   * implement Mean Average precision
   */
  private static final double EPS = 10e-6;

  @Override
  public Pair<Integer, Double> evaluate(List<Pair<String, Double>> items,
      int topK, int itemCount, double negativePref) {
    int prefCnt = 0;
    double averagePrecisionSum = 0.0;
    int sz = Math.min(topK, items.size());
    for (int i = 0; i < sz; i++) {
      Pair<String, Double> item  = items.get(i);
      double realRate = item.getSecond();
      boolean isThisPositivePref = false;
      if (realRate > negativePref) {
        prefCnt++;
        isThisPositivePref = true;
      }
      if (isThisPositivePref) {
        Double precAtI = prefCnt / (double)(i+1);
        averagePrecisionSum += precAtI;
      }
    }
    averagePrecisionSum /= (double) Math.min(itemCount,  topK);
    return new Pair<Integer, Double>(prefCnt, averagePrecisionSum);
  }  
}
