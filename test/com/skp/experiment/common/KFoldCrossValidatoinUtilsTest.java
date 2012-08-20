package com.skp.experiment.common;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mahout.common.Pair;
import org.junit.Before;
import org.junit.Test;

import com.skp.experiment.cf.als.hadoop.KFoldCrossValidationUtils;

public class KFoldCrossValidatoinUtilsTest {
  private ArrayList<String> seed;
  private int testSize = 10;
  
  @Before
  public void setup() {
    seed = new ArrayList<String>();
    for (int i = 0; i < testSize; i++) {
      seed.add(String.valueOf(i));
    }
  }
  private void printSeed(List<String> list) {
    for (int i = 0; i < list.size(); i++) {
      System.out.print(list.get(i) + " ");
    }
    System.out.println();
  }
  @Test
  public void testRandomSuffleInPlace() {
    for (int k = 0; k < 10; k++) {
      Map<String, Integer> counts = new HashMap<String, Integer>();
      KFoldCrossValidationUtils.randomSuffleInPlace(seed);
      // check size
      assertTrue(testSize == seed.size());
      
      for (int i = 0; i < seed.size(); i++) {
        if (counts.containsKey(seed.get(i)) == false) {
          counts.put(seed.get(i), 0);
        }
        counts.put(seed.get(i), counts.get(seed.get(i)) + 1);
      }
      // check distinct element
      for (Entry<String, Integer> e : counts.entrySet()) {
        assertTrue(e.getValue() == 1);
      }
      //printSeed(seed);
    }
  }
  @Test
  public void testSplitNth() {
    KFoldCrossValidationUtils.randomSuffleInPlace(seed);
    printSeed(seed);
    int kfold = 5;
    for (int nth = 0; nth < kfold; nth++) {
      System.out.println("Nth: " + nth);
      Pair<List<String>, List<String>> ret = KFoldCrossValidationUtils.splitNth(seed, 5, nth);
      printSeed(ret.getFirst());
      printSeed(ret.getSecond());
    }
  }
}
