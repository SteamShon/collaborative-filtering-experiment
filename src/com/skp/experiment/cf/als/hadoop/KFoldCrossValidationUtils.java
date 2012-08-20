package com.skp.experiment.cf.als.hadoop;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.mahout.common.Pair;
import org.apache.mahout.common.RandomUtils;

public class KFoldCrossValidationUtils {
  private static Random random = RandomUtils.getRandom();
  
  // O(N) random uniform distribution suffle. list should be ArrayList to be O(N)
  public static void randomSuffleInPlace(ArrayList<String> values) {
    int lastIndex = values.size();
    for (int i = 0; i < values.size(); i++) {
      int current = random.nextInt(lastIndex);
      // swap current, lastIndex-1
      String tmp = values.get(lastIndex-1);
      values.set(lastIndex-1, values.get(current));
      values.set(current, tmp);
      lastIndex--;
    }
  }
  /*
   * iterate values and return current nth window as probset and rest as trainingSet. O(N).
   */
  public static Pair<List<String>, List<String>> splitNth(List<String> values, int k, int nth) {
    int window = (int) Math.ceil(values.size() / (double) k);
    int startIdx = nth * window;
    int endIdx = Math.min((nth + 1) * window, values.size());
    List<String> trainingSet = new ArrayList<String>();
    List<String> probeSet = new ArrayList<String>();
    for (int i = 0; i < values.size(); i++) {
      if (i >= startIdx && i < endIdx) {
        // goes to probe set
        probeSet.add(values.get(i));
      } else {
        // rest goes to training set
        trainingSet.add(values.get(i));
      }
    }
    return new Pair<List<String>, List<String>>(trainingSet, probeSet);
  }
}
