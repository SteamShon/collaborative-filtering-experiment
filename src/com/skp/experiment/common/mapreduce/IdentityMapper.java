/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class IdentityMapper<K, V> extends Mapper<K, V, K, V> {
  public static String VALUE_ONLY_OUT = IdentityMapper.class.getName() + ".valueOnlyOut";
  public static String KEY_ONLY_OUT = IdentityMapper.class.getName() + ".keyOnlyOut";
  private static boolean valueOnlyOut = false;
  private static boolean keyOnlyOut = false;
  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    valueOnlyOut = context.getConfiguration().getBoolean(VALUE_ONLY_OUT, false);
    keyOnlyOut = context.getConfiguration().getBoolean(KEY_ONLY_OUT, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void map(K key, V value, Context context) throws IOException,
      InterruptedException {
    if (valueOnlyOut) {
      context.write((K)NullWritable.get(), value); 
    } else if (keyOnlyOut){
      context.write(key, (V)NullWritable.get());
    } else {
      context.write(key, value);
    }
  }

}
