package com.skp.experiment.cf.evaluate.hadoop;

import java.util.List;

import org.apache.mahout.common.Pair;

public interface Evaluator {
  //
  public Pair<Integer, Double> evaluate(List<Pair<String, Double>> items, 
      int topK, int itemCount, double negativePref);
  
}
