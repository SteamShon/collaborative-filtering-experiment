package com.skp.experiment.cf.als.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.common.TopK;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.mapreduce.VectorSumReducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.RowSimilarityJob;
import org.apache.mahout.math.map.OpenIntObjectHashMap;

import com.skp.experiment.cf.evaluate.hadoop.EvaluatorUtil;
import com.skp.experiment.common.HadoopClusterUtil;
import com.skp.experiment.common.Text2DistributedRowMatrixJob;
import com.skp.experiment.common.join.ImprovedRepartitionJoinAndFilterJob;
import com.skp.experiment.common.mapreduce.IdentityMapper;
import com.skp.experiment.common.mapreduce.ToVectorAndPrefReducer;
import com.skp.experiment.math.als.hadoop.ImplicitFeedbackAlternatingLeastSquaresReasonSolver;
/* 
 * 1. vectorize recommendations.
 * 2. convert recommendations vector into VectorOrPrefWritable
 * 3. convert training into VectorOrPrefWritable
 * 4. merge 2, 3 and convert into VectorAndPrefsWritable
 * 5. reasoning
 *  
 */
public class DistributedRecommenderReasonJob extends AbstractJob {
  public static final String NUM_FEATURES = DistributedRecommenderReasonJob.class.getName() + ".numFeatures";
  public static final String LAMBDA = DistributedRecommenderReasonJob.class.getName() + ".lambda";
  public static final String ALPHA = DistributedRecommenderReasonJob.class.getName() + ".alpha";
  public static final String FEATURE_MATRIX = DistributedRecommenderReasonJob.class.getName() + ".featureMatrix";
  public static final String TOP_K_SIMILARITY = DistributedRecommenderReasonJob.class.getName() + ".topKSimilarity";
  public static final String ITEM_META = DistributedRecommenderReasonJob.class.getName() + ".itemMeta";
  public static final String ITEM_ITEM_SIMILARITY = DistributedRecommenderReasonJob.class.getName() + ".itemItemSimilarity";
  public static final String SIMILARITY_WEIGHT = DistributedRecommenderReasonJob.class.getName() + ".similarityWeight";
  public static final String CATEGORY_WEIGHT = DistributedRecommenderReasonJob.class.getName() + ".categoryWeight";
  public static final String DELIMETER = ",";
  public static enum REASON_COUNTER {
    RANDOM_REASON,
    SIMILARITY_REASON,
    CATEGORY_REASON
  };
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DistributedRecommenderReasonJob(), args);
  }
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("trainings", null, "path to training set");
    addOption("itemFeatures", null, "path to the item feature matrix");
    addOption("indexPath", null, "path to index.");
    addOption("indexSize", null, "path to the index size.");
    addOption("numFeatures", null, "number of latent features.", String.valueOf(30));
    
    addOption("topKSimilarity", "topK", "top K similarity per item", String.valueOf(100));
    
    addOption("itemMeta", null, "path to item meta", true);
    addOption("itemIndex", null, "path to item index", true);
    addOption("similarityWeight", "simW", "similarity weight.", String.valueOf(1));
    addOption("categoryWeight", "cateW", "weight for category match.", String.valueOf(1));
    addOption("associationRule", "ar", "path to association rules", null);
    
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    Path recommendationsPath = getInputPath();
    Path trainingPath = new Path(getOption("trainings"));
    Path itemFeatures = new Path(getOption("itemFeatures"));
    Path itemSimPath = getTempPath("itemSims");

    //minInputSplitSize = getConf().getLong("mapred.min.split.size", HadoopClusterUtil.DEFALUT_INPUT_SPLIT_SIZE); 
    //getConf().setLong("mapred.min.split.size", HadoopClusterUtil.DEFALUT_INPUT_SPLIT_SIZE);
    
    Map<String, String> indexsSize = EvaluatorUtil.fetchTextFile(new Path(getOption("indexSize")), 
        DELIMETER, Arrays.asList(0), Arrays.asList(1));
    
    if (getOption("associationRule") == null) {
      getItemItemSimilarity(trainingPath, itemSimPath, Integer.parseInt(indexsSize.get("0")), 
          Integer.parseInt(getOption("topKSimilarity")));
    } else {
      String indexStr = getOption("indexPath") + "/1";
      getAssociationRules(new Path(getOption("associationRule")), new Path(indexStr), itemSimPath);
    }
    
    
    /* 1. vectorize recommendations */
    ToolRunner.run(new Text2DistributedRowMatrixJob(), new String[]{
      "-i", recommendationsPath.toString(), "-o", getTempPath("recommendation.vector").toString(),
      "-ri", "0", "-ci", "1", "-vi", "2"
    });
    
    Job recommendJob = prepareJob(getTempPath("recommendation.vector"), getTempPath("recommendation.conv"), 
        SequenceFileInputFormat.class, Vector2VectorOrPrefWritableMapper.class, 
        IntWritable.class, VectorOrPrefWritable.class, 
        SequenceFileOutputFormat.class);
    recommendJob.waitForCompletion(true);
    // 2. convert ratings into vectorOrPrefWritable
    Job ratingJob = prepareJob(trainingPath, getTempPath("trainings"), TextInputFormat.class,
        Text2VectorOrPrefWritableMapper.class, IntWritable.class, VectorOrPrefWritable.class,
        SequenceFileOutputFormat.class);
    ratingJob.waitForCompletion(true);
    
    // 3. merge recommendations and ratings into VectorAndPrefsWritable
    Job mergeJob = prepareJob(getTempPath("recommendation.conv"), getTempPath("merged"), 
        SequenceFileInputFormat.class, 
        IdentityMapper.class, IntWritable.class, VectorOrPrefWritable.class, 
        ToVectorAndPrefReducer.class, IntWritable.class, VectorAndPrefsWritable.class, 
        SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(mergeJob, getTempPath("trainings"));
    mergeJob.waitForCompletion(true);
    // 4. build item meta
    Path mergedItemMeta = getTempPath("mergedItemMeta");
    ToolRunner.run(new ImprovedRepartitionJoinAndFilterJob(), new String[]{
      "-i", getOption("itemIndex"), "-o", mergedItemMeta.toString(), 
      "--srcKeyIndex", "1", "-tgt", getOption("itemMeta") + ":1:0:2:inner", 
      "--mapOnly", "true"
    });
    // 5. now prefs refer to ratings and vector refers to recommendations
    Job reasonJob = prepareJob(getTempPath("merged"), getOutputPath(), SequenceFileInputFormat.class, 
        SolveImplicitFeedbackReasonSolverMapper.class, NullWritable.class, Text.class, 
        TextOutputFormat.class);
    reasonJob.getConfiguration().setLong("mapred.task.timeout", 600000 * 6 * 10);
    reasonJob.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    reasonJob.getConfiguration().setLong("mapred.min.split.size", HadoopClusterUtil.getMinInputSplitSizeMax(getConf(), getTempPath("merged")));
    reasonJob.getConfiguration().setLong("mapred.max.split.size", HadoopClusterUtil.getMinInputSplitSizeMax(getConf(), getTempPath("merged")));
    reasonJob.getConfiguration().setInt("mapred.map.tasks", HadoopClusterUtil.getNumberOfTaskTrackers(getConf()) * 100000);
    
    reasonJob.getConfiguration().set("mapred.child.java.opts", "-Xmx2g");
    reasonJob.getConfiguration().set("mapred.map.child.java.opts", "-Xmx2g");
    //reasonJob.getConfiguration().set("lock.file", getTempPath("hosts").toString());
    reasonJob.getConfiguration().set(ITEM_ITEM_SIMILARITY, itemSimPath.toString());
    reasonJob.getConfiguration().set(ITEM_META, mergedItemMeta.toString());
    reasonJob.getConfiguration().setInt(TOP_K_SIMILARITY, Integer.parseInt(getOption("topKSimilarity")));
    reasonJob.getConfiguration().setFloat(SIMILARITY_WEIGHT, Float.parseFloat(getOption("similarityWeight")));
    reasonJob.getConfiguration().setFloat(CATEGORY_WEIGHT, Float.parseFloat(getOption("categoryWeight")));
    reasonJob.waitForCompletion(true);
    
    return 0;
  }
  
  private void getItemItemSimilarity(Path train, Path output, int numCols, int topK) throws Exception {
    Job vectorizeJob = prepareJob(train, getTempPath("vectorized"), TextInputFormat.class,
        ItemBooleanRatingVectorMapper.class, IntWritable.class, VectorWritable.class, 
        VectorSumReducer.class, IntWritable.class, VectorWritable.class, 
        SequenceFileOutputFormat.class);
    vectorizeJob.setCombinerClass(VectorSumReducer.class);
    vectorizeJob.waitForCompletion(true);
    
    RowSimilarityJob rowSimJob = new RowSimilarityJob();
    rowSimJob.setConf(getConf());
    rowSimJob.getConf().setInt("io.sort.factor", 100);
    rowSimJob.getConf().setLong("mapred.task.timeout", 600000 * 60);
    rowSimJob.getConf().setBoolean("mapred.map.tasks.speculative.execution", false);
    rowSimJob.run(new String[]{
        "-i", getTempPath("vectorized").toString(), "-o", output.toString(), 
        "--numberOfColumns", String.valueOf(numCols),
        "--similarityClassname", "SIMILARITY_COOCCURRENCE", 
        "--maxSimilaritiesPerRow", String.valueOf(topK),
        "--excludeSelfSimilarity", "true", 
        "--tempDir", getTempPath("similarity").toString()
    });
  }
  private void getAssociationRules(Path arPath, Path indexPath, Path output) throws Exception {
    ToolRunner.run(new ImprovedRepartitionJoinAndFilterJob(), new String[]{
      "-i", arPath.toString(), "-o", getTempPath("firstSim").toString(), 
      "-sidx", "0", "-tgt", indexPath.toString() + ":0:1:0:sub", "--mapOnly", "true"
    });
    ToolRunner.run(new ImprovedRepartitionJoinAndFilterJob(), new String[]{
      "-i", getTempPath("firstSim").toString(), "-o", getTempPath("secondSim").toString(), 
      "-sidx", "1", "-tgt", indexPath.toString() + ":1:1:0:sub", "--mapOnly", "true"
    });
    ToolRunner.run(new Text2DistributedRowMatrixJob(), new String[]{
      "-i", getTempPath("secondSim").toString(), "-o", output.toString(), 
      "-ri", "0", "-ci", "1", "-vi", "2"
    });
  }
  public static class ItemBooleanRatingVectorMapper extends 
    Mapper<LongWritable, Text, IntWritable, VectorWritable> {
    private static IntWritable rowID = new IntWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(value.toString());
      int userID = Integer.parseInt(tokens[0]);
      int itemID = Integer.parseInt(tokens[1]);
      float rating = 1;
      Vector ratings = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
      ratings.set(userID, rating);
      rowID.set(itemID);
      context.write(rowID, new VectorWritable(ratings));
    }
    
  }
  public static class Text2VectorOrPrefWritableMapper extends 
    Mapper<LongWritable, Text, IntWritable, VectorOrPrefWritable> {
    private static IntWritable outKey = new IntWritable();
    
    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(line.toString());
      int userID = Integer.parseInt(tokens[0]);
      int itemID = Integer.parseInt(tokens[1]);
      float rating = Float.parseFloat(tokens[2]);
      outKey.set(userID);
      context.write(outKey, new VectorOrPrefWritable((long)itemID, rating));
    }
  }
  public static class Vector2VectorOrPrefWritableMapper extends
    Mapper<IntWritable, VectorWritable, IntWritable, VectorOrPrefWritable> {
    @Override
    protected void map(IntWritable userID, VectorWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(userID, new VectorOrPrefWritable(value.get()));
    }
  }
  public static class SolveImplicitFeedbackReasonSolverMapper extends 
    Mapper<IntWritable, VectorAndPrefsWritable, NullWritable, Text> {
    
    private ImplicitFeedbackAlternatingLeastSquaresReasonSolver solver;
    private static Text outValue = new Text();
    private OpenIntObjectHashMap<TopK<RecommendedItem>> itemSims;
    private Map<Integer, String> itemMetas = new HashMap<Integer, String>();
    private float simWeight = 0;
    private float cateWeight = 0;
    private Random random;
    @Override
    protected void setup(Context ctx) throws IOException,
        InterruptedException {
      random = RandomUtils.getRandom();
      int topK = ctx.getConfiguration().getInt(TOP_K_SIMILARITY, 100);
      Path YPath = new Path(ctx.getConfiguration().get(ITEM_ITEM_SIMILARITY));
      Path itemMetaPath = new Path(ctx.getConfiguration().get(ITEM_META));
      itemSims = ALSMatrixUtil.readMatrixByRowsInOrder(YPath, ctx, topK);
      
      for (Entry<String, String> item : 
        EvaluatorUtil.fetchTextFiles(ctx, itemMetaPath, DELIMETER, 
            Arrays.asList(0), Arrays.asList(1)).entrySet()) {
        itemMetas.put(Integer.parseInt(item.getKey()), item.getValue());
      }
      ctx.setStatus("Item Sims: " + itemSims.size());
      simWeight = ctx.getConfiguration().getFloat(SIMILARITY_WEIGHT, 2);
      cateWeight = ctx.getConfiguration().getFloat(CATEGORY_WEIGHT, 1);
      
    }
    static <K,V extends Comparable<? super V>>
    SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
      SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
          new Comparator<Map.Entry<K,V>>() {
            @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
              return e1.getValue().compareTo(e2.getValue());
            }
          }
          );
      sortedEntries.addAll(map.entrySet());
      return sortedEntries;
    }
    // get distance between x, and y, assumes lenth of x, and y is same
    private float getDistance(String x, String y) {
      int common = 0;
      for (int i = 0; i < x.length() && i < y.length(); i++) {
        if (x.charAt(i) != y.charAt(i)) {
          break;
        }
        common++;
      }
      return common / (float)x.length();
    }
    @Override
    protected void map(IntWritable userIDWritable, VectorAndPrefsWritable recAndRatings,
        Context context) throws IOException, InterruptedException {
      List<Long> ratedItemIDs = recAndRatings.getUserIDs();
      List<Float> ratedScores = recAndRatings.getValues();
      Vector recommendations = recAndRatings.getVector();
      Map<Integer, Boolean> ratedItemIDsMap = new HashMap<Integer, Boolean>();
      
      for (Long id : ratedItemIDs) {
        ratedItemIDsMap.put(id.intValue(), true);
      }
      
      Iterator<Vector.Element> recs = recommendations.iterateNonZero();
      while (recs.hasNext()) {
        Vector.Element rec = recs.next();
        Map<Long, Float> candidatesMap = new HashMap<Long, Float>();
        Map<Long, Float> simMap = new HashMap<Long, Float>();
        List<RecommendedItem> simList = new ArrayList<RecommendedItem>();
        
        // get best reason for this recommendation
        int bestItemId = -1;
        //double bestItemScore = -1.0;
        float bestItemScore = 0;
        //check no rules for this rec_id item
        if (itemSims.containsKey(rec.index())) {
          simList = itemSims.get(rec.index()).retrieve();
        }
        //build similarity map
        for (RecommendedItem item : simList) {
          simMap.put(item.getItemID(), item.getValue());
        }
        
        for (Long ratedItemID : ratedItemIDs) {
          float curCandidateScore = 0;
          if (simMap.containsKey(ratedItemID)) {
            curCandidateScore += simWeight + simMap.get(ratedItemID);
          }
          if (itemMetas.containsKey(rec.index()) && itemMetas.containsKey(ratedItemID.intValue())) {
            curCandidateScore += cateWeight * getDistance(itemMetas.get(rec.index()), itemMetas.get(ratedItemID.intValue()));
          }
          candidatesMap.put(ratedItemID, curCandidateScore);
        }
        for (Entry<Long, Float> candidate : candidatesMap.entrySet()) {
          if (bestItemScore < candidate.getValue()) {
            bestItemId = candidate.getKey().intValue();
            bestItemScore = candidate.getValue();
            if (bestItemScore >= simWeight * 1) {
              context.getCounter(REASON_COUNTER.SIMILARITY_REASON).increment(1);  
            } else if (bestItemScore > 0) {
              context.getCounter(REASON_COUNTER.CATEGORY_REASON).increment(1);
            }
          }
        }
        // no match for similarity, category. just randomly pick 
        if (bestItemId == -1) {
          context.getCounter(REASON_COUNTER.RANDOM_REASON).increment(1);
          bestItemId = ratedItemIDs.get(random.nextInt(ratedItemIDs.size())).intValue();
        }
        outValue.set(userIDWritable.get() + DELIMETER + rec.index() + DELIMETER +
            rec.get() + DELIMETER + bestItemId);
        context.write(NullWritable.get(), outValue);
      }
    }
  }
}
