package com.skp.experiment.cf.evaluate.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


/* 
 * This class evaluate recommendations with flag indicating existence in validation set. 
 */
public class EvaluatorJob extends AbstractJob {
  private static final String DELIMETER = ",";
  private static final String RECOMMENDATIONS_PER_USER = EvaluatorJob.class.getName() + ".recommendationsPerUser";
  protected static double negativePref = -1.0;
  
  protected Path trainingSetPath;
  protected Path validationSetPath;
  protected Path recommendationSetPath;
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new EvaluatorJob(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("topK", "k", "recommendations per user.", false);
    addOption("cleanUp", null, "true if only want stat, otherwise false", String.valueOf(false));
    if (parseArguments(args) == null) {
      return -1;
    }
    
    Job evaluateJob = prepareJob(getInputPath(), getOutputPath(), TextInputFormat.class, 
        ToVectorizeMapper.class, RecommendationsKey.class, Text.class, 
        MergeTopKRecommendationsReducer.class, NullWritable.class, Text.class, 
        TextOutputFormat.class);
    Configuration conf = evaluateJob.getConfiguration();
    conf.setInt(RECOMMENDATIONS_PER_USER, Integer.parseInt(getOption("topK")));
    
    evaluateJob.setPartitionerClass(RecommendationsKeyPartitioner.class);
    evaluateJob.setSortComparatorClass(RecommendationsKeyComparator.class);
    evaluateJob.setGroupingComparatorClass(RecommendationsKeyGroupingComparator.class);
    
    evaluateJob.waitForCompletion(true);
    writeOutEvaluationMetrics(getOutputPath()); 
    if (Boolean.parseBoolean(getOption("cleanUp")) == true) {
      EvaluatorUtil.deletePartFiles(getConf(), getOutputPath());
    }
    return 0;
  }
  
  /* aggregate average precision per user and create final result*/
  public void writeOutEvaluationMetrics(Path output) throws IOException {
    Map<Integer, Double> stats = 
        EvaluatorUtil.getResultSumPerColumns(getConf(), output, Arrays.asList(0), true);
    StringBuffer sb = new StringBuffer();
    double totalCount = stats.get(EvaluatorUtil.RECORD_COUNT_SUM_INDEX);
    TreeSet<Integer> keys = new TreeSet<Integer>(stats.keySet());
    for (Integer statKey : keys) {
      if (statKey != EvaluatorUtil.RECORD_COUNT_SUM_INDEX) {
        sb.append(DELIMETER);
      }
      if (statKey < 3) {
        sb.append(stats.get(statKey).intValue());
      } else {
        sb.append(stats.get(statKey) / totalCount);
      }
    }
    EvaluatorUtil.writeToHdfs(getConf(), getOutputPath("_stats"), sb.toString());
  }
 
  /** use secondary sort here */
  private static class ToVectorizeMapper extends 
    Mapper<LongWritable, Text, RecommendationsKey, Text> {
    private static Text outValue = new Text();
    private static RecommendationsKey outKey = new RecommendationsKey();
    
    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(line.toString());
      try {
        int sz = tokens.length;
        String userID = tokens[0];
        String itemID = tokens[1];
        float rating = Float.parseFloat(tokens[2]);
        double flag = Double.parseDouble(tokens[sz-2]);
        int itemCount = Integer.parseInt(tokens[sz-1]);
        outValue.set(itemID + DELIMETER + rating + DELIMETER + flag);
        outKey.set(userID, rating, itemCount);
        context.write(outKey, outValue);
      } catch (NumberFormatException e) {
        return;
      }
      
    }
  }
  private static class MergeTopKRecommendationsReducer extends 
    Reducer<RecommendationsKey, Text, NullWritable, Text> {
    
    private static List<Evaluator> evaluators;
    private static Text outValue = new Text();
    private static int topK = 0;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      topK = context.getConfiguration().getInt(RECOMMENDATIONS_PER_USER, 0);
      evaluators = 
          Arrays.asList(new PrecisionEvaluator(), new MeanAveragePrecisionEvaluator(), 
              new ExpectedPercentileRankEvaluator(), new RecallEvaluator());
    }
    
    @Override
    protected void reduce(RecommendationsKey compositeKey, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
      String userID = compositeKey.getUserID();
      int itemCount = compositeKey.getItemCount();
      List<Pair<String, Double>> items = new ArrayList<Pair<String, Double>>();
      
      for (Text value: values) {
        String[] tokens = value.toString().split(DELIMETER);
        String itemID = tokens[0];
        double flag = Double.parseDouble(tokens[2]);
        Pair<String, Double> newPair = new Pair<String, Double>(itemID, flag);
        items.add(newPair);
      }
      List<Pair<Integer, Double>> scores = new ArrayList<Pair<Integer, Double>>();
      for (Evaluator evaluator : evaluators) {
        Pair<Integer, Double> curScore = evaluator.evaluate(items, topK, itemCount, negativePref);
        scores.add(curScore);
      }
      outValue.set(userID + DELIMETER + itemCount + DELIMETER + scores.get(0).getFirst() 
          + DELIMETER + buildOutput(scores));
      context.write(NullWritable.get(), outValue);
    }
    private String buildOutput(List<Pair<Integer, Double>> scores) {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < scores.size(); i++) {
        if (i != 0) {
          sb.append(DELIMETER);
        }
        sb.append(scores.get(i).getSecond());
      }
      return sb.toString();
    }
  }
}
