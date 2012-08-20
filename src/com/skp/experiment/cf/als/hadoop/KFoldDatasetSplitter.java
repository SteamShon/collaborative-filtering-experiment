/**
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

package com.skp.experiment.cf.als.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;

import com.skp.experiment.common.HadoopClusterUtil;
import com.skp.experiment.common.parameter.DefaultOptionCreator;

/**
 * <p>Split a recommendation dataset into a training and a test set</p>
 * <p>randomly pick P population from user`s 
 *
  * <p>Command line arguments specific to this class are:</p>
 *
 * <ol>
 * <li>--input (path): Directory containing one or more text files with the dataset</li>
 * <li>--output (path): path where output should go</li>
 * <li>--trainingPercentage (double): percentage of the data to use as training set (optional, default 0.9)</li>
 * <li>--probePercentage (double): percentage of the data to use as probe set (optional, default 0.1)</li>
 * </ol>
 */
public class KFoldDatasetSplitter extends AbstractJob {
  private static final String KEY_INDEX = KFoldDatasetSplitter.class.getName() + ".keyIndex";
  private static final String K_FOLD = DatasetSplitter.class.getName() + ".kFold";
  private static final String PROBE_SET = DatasetSplitter.class.getName() + ".probeSet";
  private static final String TRAIN_SET = DatasetSplitter.class.getName() + ".trainSet";
  private static final int DEFAULT_K_FOLD = 4;
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new KFoldDatasetSplitter(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    addInputOption();
    addOutputOption();
    addOption("kfold", "k", "number of fold for cross validation.", String.valueOf(DEFAULT_K_FOLD));
    
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    Path markedPrefs = new Path(getOption("tempDir"), "markedPreferences");
    Path trainingSetPath = new Path(getOutputPath(), "trainingSet");
    Path probeSetPath = new Path(getOutputPath(), "probeSet");
    int kFold = Integer.parseInt(getOption("kfold"));
    
    /** step0. build trainingSet/probeSet pair as (1-P, P) probability for each K fold. */
    Job markPreferences = prepareJob(getInputPath(), markedPrefs, TextInputFormat.class,
        MarkPreferencesMapper.class, Text.class, Text.class, 
        MarkPreferencesReducer.class, NullWritable.class, Text.class, 
        TextOutputFormat.class);
   
    markPreferences.getConfiguration().setInt(KEY_INDEX, Integer.parseInt(getOption("keyIndex")));
    markPreferences.getConfiguration().setInt(K_FOLD, kFold);
    markPreferences.getConfiguration().set(TRAIN_SET, trainingSetPath.toString());
    markPreferences.getConfiguration().set(PROBE_SET, probeSetPath.toString());
    markPreferences.waitForCompletion(true);
    
    return 0;
  }
  
  static class MarkPreferencesMapper extends Mapper<LongWritable,Text,Text,Text> {
    private static Text outKey = new Text();
    private static int keyIndex = 0;
    
    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      keyIndex = ctx.getConfiguration().getInt(KEY_INDEX, 0);
    }

    @Override
    protected void map(LongWritable key, Text text, Context ctx) throws IOException, InterruptedException {
      outKey.set(TasteHadoopUtils.splitPrefTokens(text.toString())[keyIndex]);
      ctx.write(outKey, text);
    }
  }
  static class MarkPreferencesReducer extends Reducer<Text, Text, Text, Text> {
    private int kfold;
    private static List<FSDataOutputStream> trainStreams = new ArrayList<FSDataOutputStream>();
    private static List<FSDataOutputStream> probeStreams = new ArrayList<FSDataOutputStream>();
    private static FileSystem fs;
    
    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      kfold = ctx.getConfiguration().getInt(K_FOLD, DEFAULT_K_FOLD);
      
      fs = FileSystem.get(ctx.getConfiguration());
      String taskId = HadoopClusterUtil.getAttemptId(ctx.getConfiguration());
      /** populate kfold number of stream use taskId to make sure no collison on file name */
      for (int i = 0; i < kfold; i++) {
        Path curProbeSetPath = new Path(ctx.getConfiguration().get(PROBE_SET), i + "/" + taskId);
        Path curTrainSetPath = new Path(ctx.getConfiguration().get(TRAIN_SET), i + "/" + taskId); 
        trainStreams.add(fs.create(curTrainSetPath, true));
        probeStreams.add(fs.create(curProbeSetPath, true));
      }
    }
    /**
     * delete all open Writer.
     */
    @Override
    protected void cleanup(Context context) 
        throws IOException, InterruptedException {
      for (FSDataOutputStream s : trainStreams) {
        IOUtils.closeStream(s);
      }
      for (FSDataOutputStream s : probeStreams) {
        IOUtils.closeStream(s);
      }
    }
    /**
     * list of item:rating per this user
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
        throws IOException, InterruptedException {
      ArrayList<String> list = new ArrayList<String>();
      for (Text value : values) {
        list.add(value.toString());
      }
      // random shuffle 
      // space complexity: O(|# items for this user| x 2).
      KFoldCrossValidationUtils.randomSuffleInPlace(list); 
      for (int i = 0; i < kfold; i++) {
        /** get current fold as probeSet */
        Pair<List<String>, List<String>> trainingAndProbe = KFoldCrossValidationUtils.splitNth(list, kfold, i);
        List<String> trains = trainingAndProbe.getFirst();
        List<String> probes = trainingAndProbe.getSecond();
       
        /** flush out current k'th fold training/probeset pair into filestream */
        for (int t = 0; t < trains.size(); t++) {
          trainStreams.get(i).writeBytes(trains.get(t) + DefaultOptionCreator.NEWLINE);
        }
        for (int p = 0; p < probes.size(); p++) {
          probeStreams.get(i).writeBytes(probes.get(p) + DefaultOptionCreator.NEWLINE);
        }
      }
    }
  }
}
