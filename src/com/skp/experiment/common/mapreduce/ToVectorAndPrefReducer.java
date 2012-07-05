/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.skp.experiment.common.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.Vector;

import com.google.common.collect.Lists;

public final class ToVectorAndPrefReducer extends
    Reducer<IntWritable,VectorOrPrefWritable,IntWritable,VectorAndPrefsWritable> {

  @Override
  protected void reduce(IntWritable key,
                        Iterable<VectorOrPrefWritable> values,
                        Context context) throws IOException, InterruptedException {

    List<Long> userIDs = Lists.newArrayList();
    List<Float> prefValues = Lists.newArrayList();
    Vector similarityMatrixColumn = null;
    for (VectorOrPrefWritable value : values) {
      if (value.getVector() == null) {
        // Then this is going into list
        userIDs.add(value.getUserID());
        prefValues.add(value.getValue());
      } else {
        // Then this is going into vector
        if (similarityMatrixColumn != null) {
          throw new IllegalStateException(" " + key.get());
        }
        similarityMatrixColumn = value.getVector();
      }
    }

    if (similarityMatrixColumn == null) {
      return;
    }

    VectorAndPrefsWritable vectorAndPrefs = new VectorAndPrefsWritable(similarityMatrixColumn, userIDs, prefValues);
    context.write(key, vectorAndPrefs);
  }

}
