package com.skp.experiment.cf.als.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.skp.experiment.cf.evaluate.hadoop.EvaluatorUtil;

public class FilterInputJob extends AbstractJob {
  private static final Logger log = LoggerFactory.getLogger(FilterInputJob.class);
  private static final String INVALID_ITEM_PATH = FilterInputJob.class.getName() + ".invalidItemPath";
  private static final String MIN_NON_DEFAULT_ELEMENTS_NUM = 
      FilterInputJob.class.getName() + ".minNonDefaultElementsNum";
  private static final String[] PROHIBIT_PREFIX_ON_ITEM_IDS = new String[]{"R", "H"}; 
  
  private static final String DELIMETER = ",";
  private static final String ITEM_INDEX = FilterInputJob.class.getName() + ".itemIndex";
  private static final String USER_INDEX = FilterInputJob.class.getName() + ".userIndex";
  /*
  private static final String TRAINING_PERCENTAGE = FilterInputJob.class.getName() + ".trainingPercentage";
  private static final String PROBE_PERCENTAGE = FilterInputJob.class.getName() + ".probePercentage";
  private static final String TRAINING_PATH = FilterInputJob.class.getName() + ".trainingPath";
  private static final String PROBE_PATH = FilterInputJob.class.getName() + ".probePath";
  private static final double DEFAULT_TRAINING_PERCENTAGE = 0.9;
  private static final double DEFAULT_PROBE_PERCENTAGE = 0.1;
  */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new FilterInputJob(), args);
  }
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("invalidItemPath", null, "invalid item path.", null);
    addOption("minNonDefaultElementsNum", "minK", "minimum number of non default elements in result vector.", String.valueOf(0));
    addOption("itemIndex", "itemIdx", "item id index", String.valueOf(1));
    addOption("userIndex", "userIdx", "user id index", String.valueOf(0));
    /*
    addOption("trainingPercentage", "t", "percentage of the data to use as training set", 
        String.valueOf(DEFAULT_TRAINING_PERCENTAGE));
    addOption("probePercentage", "p", "percentage of the data to use as probe set", 
        String.valueOf(DEFAULT_PROBE_PERCENTAGE));
    */
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    Job filterJob = prepareJob(getInputPath(), getOutputPath(), TextInputFormat.class, 
        FilterInvalidItemMapper.class, Text.class, Text.class, 
        FilterInvalidItemReducer.class, NullWritable.class, Text.class, 
        TextOutputFormat.class
        );
    //Path trainingPath = new Path(getOutputPath().getParent(), "trainingSet");
    //Path probePath = new Path(getOutputPath().getParent(), "probeSet");
    filterJob.getConfiguration().set(INVALID_ITEM_PATH, getOption("invalidItemPath"));
    filterJob.getConfiguration().set(MIN_NON_DEFAULT_ELEMENTS_NUM, getOption("minNonDefaultElementsNum"));
    filterJob.getConfiguration().setInt(ITEM_INDEX, Integer.parseInt(getOption("itemIndex")));
    filterJob.getConfiguration().setInt(USER_INDEX, Integer.parseInt(getOption("userIndex")));
    /*
    filterJob.getConfiguration().set(TRAINING_PATH, trainingPath.toString());
    filterJob.getConfiguration().set(PROBE_PATH, probePath.toString());
    filterJob.getConfiguration().setFloat(TRAINING_PERCENTAGE, Float.parseFloat(getOption("trainingPercentage")));
    filterJob.getConfiguration().setFloat(PROBE_PERCENTAGE, Float.parseFloat(getOption("probePercentage")));
    */
    filterJob.waitForCompletion(true);
    return 0;
  }
  
  private static class FilterInvalidItemMapper 
    extends Mapper<LongWritable, Text, Text, Text> {
    
    private static Map<String, String> invalidItems = null;
    private static Text outKey = new Text();
    private static Text outValue = new Text();
    private static Integer itemIndex = 0;
    private static Integer userIndex = 0;
    @Override
    protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(line.toString());
      String userID = tokens[userIndex];
      String itemID = tokens[itemIndex];
      
      if (invalidItems == null || invalidItems.containsKey(itemID) == false) {
        boolean isProhibit = false;
        for (String prefix : PROHIBIT_PREFIX_ON_ITEM_IDS) {
          if (itemID.startsWith(prefix)) {
            log.info("Prohibit ItemID: {}", itemID);
            isProhibit = true;
            break;
          }
        }
        if (!isProhibit) {
          outKey.set(userID);
          outValue.set(line);
          ctx.write(outKey, outValue);
        }
      }
    }

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      itemIndex = conf.getInt(INVALID_ITEM_PATH, 1);
      userIndex = conf.getInt(USER_INDEX, 0);
      
      if (conf.get(INVALID_ITEM_PATH) != null) {
        invalidItems = ALSMatrixUtil.fetchTextFiles(context, 
            new Path(conf.get(INVALID_ITEM_PATH)), 
            DELIMETER, Arrays.asList(itemIndex), Arrays.asList(itemIndex));
        context.setStatus("total: " + invalidItems.size());
      }
    }
  }
  
  private static class FilterInvalidItemReducer 
    extends Reducer<Text, Text, NullWritable, Text> {
    
    private static int minNonDefaultElementsNum = 0;
    private static Text outValue = new Text();
    /*
    private Random random;
    private float trainingBound;
    private float probeBound;
    private Path trainingPath;
    private Path probePath;
    //private SequenceFile.Writer trainingWriter = null;
    //private SequenceFile.Writer probeWriter = null;
    private FSDataOutputStream trainingOut = null;
    private FSDataOutputStream probeOut = null;
    
    private String getPartNum(Context context) {
      String taskId = context.getConfiguration().get("mapred.task.id");
      String[] parts = taskId.split("_");
      return "part-" + parts[parts.length - 2] + "-" + parts[parts.length - 1];
    }
    */
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      minNonDefaultElementsNum = conf.getInt(MIN_NON_DEFAULT_ELEMENTS_NUM, -1);
      /*
      random = RandomUtils.getRandom();
      trainingBound = conf.getFloat(TRAINING_PERCENTAGE, (float)DEFAULT_TRAINING_PERCENTAGE);
      probeBound = trainingBound + conf.getFloat(PROBE_PERCENTAGE, (float)DEFAULT_PROBE_PERCENTAGE);
      trainingPath = new Path(conf.get(TRAINING_PATH), getPartNum(context));
      probePath = new Path(conf.get(PROBE_PATH), getPartNum(context));
      trainingOut = fs.create(trainingPath);
      probeOut = fs.create(probePath);
      */
      
    }
    @Override
    protected void reduce(Text user, Iterable<Text> lines, Context context) throws IOException,
        InterruptedException {
      List<String> aggregated = new ArrayList<String>();
      for (Text line : lines) {
        aggregated.add(line.toString());
      }
      if (aggregated.size() < minNonDefaultElementsNum) {
        return;
      }
      for (String s : aggregated) {
        outValue.set(s);
        /*
        double randomValue = random.nextDouble();
        if (randomValue <= trainingBound) {
          trainingOut.writeUTF(s);
          //trainingWriter.append(NullWritable.get(), outValue);
        } else if (randomValue <= probeBound) {
          probeOut.writeUTF(s);
          //probeWriter.append(NullWritable.get(), outValue);
        }
        */
        context.write(NullWritable.get(), outValue);
      }
    }
    
  }
}
