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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.common.TopK;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.IntObjectProcedure;
import org.apache.mahout.math.map.OpenIntObjectHashMap;

import com.google.common.primitives.Floats;
import com.skp.experiment.common.HadoopClusterUtil;

/**
 * <p>Computes the top-N recommendations per user from a decomposition of the rating matrix</p>
 *
 * <p>Command line arguments specific to this class are:</p>
 *
 * <ol>
 * <li>--input (path): Directory containing the vectorized user ratings</li>
 * <li>--output (path): path where output should go</li>
 * <li>--numRecommendations (int): maximum number of recommendations per user</li>
 * <li>--maxRating (double): maximum rating of an item</li>
 * <li>--numFeatures (int): number of features to use for decomposition </li>
 * </ol>
 */
public class DistributedRecommenderJob extends AbstractJob {

  private static final String NUM_RECOMMENDATIONS = DistributedRecommenderJob.class.getName() + ".numRecommendations";
  //private static final String USER_FEATURES_PATH = DistributedRecommenderJob.class.getName() + ".userFeatures";
  private static final String ITEM_FEATURES_PATH = DistributedRecommenderJob.class.getName() + ".itemFeatures";
  private static final String MAX_RATING = DistributedRecommenderJob.class.getName() + ".maxRating";
  //private static final String USER_RATINGS_PATH = DistributedRecommenderJob.class.getName() + ".userRatings";
  private static final String USER_ITEM_COUNTS_PATH = DistributedRecommenderJob.class.getName() + ".userItemCnts";
  private static final String TEST_SET_SAMPLE_PERCENTAGE = DistributedRecommenderJob.class.getName() + ".testSetSamplePercentage";
  private static final String TEST_SEED_USER_PATH = DistributedRecommenderJob.class.getName() + ".testSeedUsersPath";
  
  private static final String DELIMETER = ",";
  //private static final String INDEX_VALUE_DELIMETER = ":";
  static final int DEFAULT_NUM_RECOMMENDATIONS = 10;
  //private static long minInputSplitSize = 1024 * 1024 * 64;
  private static final int multiplyMapTasks = 100000;
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DistributedRecommenderJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    // input file is user x feature matrix U.
    addInputOption();
    //addOption("userFeatures", null, "path to the user feature matrix", true);
    addOption("itemFeatures", null, "path to the item feature matrix", true);
    addOption("numRecommendations", null, "number of recommendations per user",
        String.valueOf(DEFAULT_NUM_RECOMMENDATIONS));
    addOption("maxRating", null, "maximum rating available", true);
    addOption("userItemCnts", null, "path to the user item count data", true);
    addOption("samplePercentage", "sample", "sampling rate.", String.valueOf(0.01));
    addOption("seedUsers", null, "path to seed test users in index format.", null);
    
    addOutputOption();
    
    Map<String,String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    //minInputSplitSize = getConf().getLong("mapred.min.split.size", HadoopClusterUtil.DEFALUT_INPUT_SPLIT_SIZE);
    
    Job prediction = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class, PredictionMapper.class,
        NullWritable.class, Text.class, TextOutputFormat.class);
    prediction.getConfiguration().setInt(NUM_RECOMMENDATIONS,
        Integer.parseInt(parsedArgs.get("--numRecommendations")));
    prediction.getConfiguration().set(ITEM_FEATURES_PATH, parsedArgs.get("--itemFeatures"));
    prediction.getConfiguration().set(MAX_RATING, parsedArgs.get("--maxRating"));
    prediction.getConfiguration().set(USER_ITEM_COUNTS_PATH, parsedArgs.get("--userItemCnts"));
    prediction.getConfiguration().set(TEST_SET_SAMPLE_PERCENTAGE, parsedArgs.get("--samplePercentage"));
    
    prediction.getConfiguration().setLong("mapred.min.split.size", HadoopClusterUtil.getMinInputSplitSizeMax(getConf(), getInputPath()));
    prediction.getConfiguration().setLong("mapred.max.split.size", HadoopClusterUtil.getMinInputSplitSizeMax(getConf(), getInputPath()));
    prediction.getConfiguration().setInt("mapred.map.tasks", HadoopClusterUtil.getNumberOfTaskTrackers(getConf()) * multiplyMapTasks);
    
    prediction.waitForCompletion(true);

    return 0;
  }
  
  public static final Comparator<RecommendedItem> BY_PREFERENCE_VALUE =
      new Comparator<RecommendedItem>() {
        @Override
        public int compare(RecommendedItem one, RecommendedItem two) {
          return Floats.compare(one.getValue(), two.getValue());
        }
      };
     
  // assumes U >> M 
  // read <user, item count> in setup phase, and adjust number of Top items.
  static class PredictionMapper
      extends Mapper<IntWritable,VectorWritable,NullWritable,Text> {
    
    private Random random;
    private double sampleBound;
    //private OpenIntObjectHashMap<Vector> U;
    private OpenIntObjectHashMap<Vector> M;
    private Map<Integer, Integer> userItemCnts;
    private Map<String, String> testSeedUsers = null;
    private Path testSeedUsersPath = null;
    
    private int testSeedUserIndexColumn = 0;
    private int recommendationsPerUser;
    private float maxRating;
    
    private static Text outValue = new Text();
    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      Configuration conf = ctx.getConfiguration();
      recommendationsPerUser = conf.getInt(NUM_RECOMMENDATIONS, DEFAULT_NUM_RECOMMENDATIONS);
      maxRating = Float.parseFloat(conf.get(MAX_RATING));
      Path userItemCntsPath = new Path(conf.get(USER_ITEM_COUNTS_PATH));
      userItemCnts = retrieveUserItemCnts(ctx, userItemCntsPath);
      
      //Path pathToU = new Path(ctx.getConfiguration().get(USER_FEATURES_PATH));
      Path pathToM = new Path(conf.get(ITEM_FEATURES_PATH));

      //U = ALSUtils.readMatrixByRows(pathToU, ctx.getConfiguration());
      //M = ALSMatrixUtil.readMatrixByRows(pathToM, ctx.getConfiguration());
      M = ALSMatrixUtil.readMatrixByRows(pathToM, ctx);
      random = RandomUtils.getRandom();
      sampleBound = Double.parseDouble(conf.get(TEST_SET_SAMPLE_PERCENTAGE));
      /*
      String testSeedUsersDir = conf.get(TEST_SEED_USER_PATH);
      if (testSeedUsersDir != null) {
        testSeedUsersPath = new Path(testSeedUsersDir);
        testSeedUsers = EvaluatorUtil.fetchTextFile(ctx, testSeedUsersPath, 
            DELIMETER, Arrays.asList(testSeedUserIndexColumn), 
            Arrays.asList(testSeedUserIndexColumn));
        ctx.setStatus("total: " + testSeedUsers.size());
      }
      */
    }
    
    private Map<Integer, Integer> retrieveUserItemCnts(Context context, Path input) throws IOException {
      Map<Integer, Integer> result = new HashMap<Integer, Integer>();
      Configuration conf = context.getConfiguration();
      SequenceFileDirIterator<IntWritable, IntWritable> iter = 
          new SequenceFileDirIterator<IntWritable, IntWritable>(new Path(input, "part*"),
              PathType.GLOB, null, null, true, conf);

      while (iter.hasNext()) {
        Pair<IntWritable, IntWritable> pair = iter.next();
        result.put(pair.getFirst().get(), pair.getSecond().get());
      }
      return result;
    }
    
    private String buildOutput(IntWritable userIDWritable, RecommendedItem item) {
      StringBuffer sb = new StringBuffer();
      sb.append(userIDWritable.get()).append(DELIMETER);
      sb.append(item.getItemID()).append(DELIMETER).append(Math.min(item.getValue(), maxRating));
      return sb.toString();
    }
    private List<RecommendedItem> normailzeItems(List<RecommendedItem> items) {
      List<RecommendedItem> norm = new ArrayList<RecommendedItem>();
      if (items.size() == 0) {
        return items;
      }
      float maxScore = items.get(0).getValue();
      float minScore = items.get(items.size()-1).getValue();
      if (maxScore - minScore == 0) {
        return items;
      }
      for (RecommendedItem item : items) {
        float normScore = (item.getValue() - minScore) / (maxScore - minScore) * (1f - 0.5f) + 0.5f;
        norm.add(new GenericRecommendedItem(item.getItemID(), normScore));
      }
      return norm;
    }
    @Override
    protected void map(IntWritable userIDWritable, VectorWritable featuresWritable, Context ctx)
        throws IOException, InterruptedException {
      boolean skipThisUser = true;
      String userIDString = String.valueOf(userIDWritable.get());
      if (testSeedUsers != null && testSeedUsers.containsKey(userIDString)) {
        skipThisUser = false;
      }
      double randomValue = random.nextDouble();
      if (randomValue > sampleBound && skipThisUser) {
        return;
      }
      //StringBuffer sb = new StringBuffer();
      final Vector features = featuresWritable.get();
      final int userID = userIDWritable.get();
      int topK = recommendationsPerUser;
      if (userItemCnts.containsKey(userID)) {
        topK += userItemCnts.get(userID);
      }
      final TopK<RecommendedItem> topKItems = new TopK<RecommendedItem>(topK, BY_PREFERENCE_VALUE);
      
      M.forEachPair(new IntObjectProcedure<Vector>() {
        @Override
        public boolean apply(int itemID, Vector itemFeatures) {
          double predictedRating = features.dot(itemFeatures);
          topKItems.offer(new GenericRecommendedItem(itemID, (float) predictedRating));
          return true;
        }
      });
      //sb.append(userID);
      List<RecommendedItem> recommendedItems = normailzeItems(topKItems.retrieve());;
      for (RecommendedItem topItem : recommendedItems) {
        //recommendedItems.add(new GenericRecommendedItem(topItem.getItemID(), Math.min(topItem.getValue(), maxRating)));
        //sb.append(DELIMETER + topItem.getItemID() + INDEX_VALUE_DELIMETER + Math.min(topItem.getValue(), maxRating));
        outValue.set(buildOutput(userIDWritable, topItem));
        ctx.write(NullWritable.get(), outValue);
      }
      //outValue.set(sb.toString());
      //ctx.write(NullWritable.get(), outValue);
    }
  }
  
}
