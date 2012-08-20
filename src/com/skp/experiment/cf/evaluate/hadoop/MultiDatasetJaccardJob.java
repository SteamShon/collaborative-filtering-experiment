package com.skp.experiment.cf.evaluate.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.AbstractJob;

import com.skp.experiment.common.HadoopClusterUtil;
import com.skp.experiment.common.OptionParseUtil;

public class MultiDatasetJaccardJob extends AbstractJob {
  private static final String DELIMETER = ",";
  private static final String VALUE_COLUMN_INDEX = MultiDatasetJaccardJob.class.getName() + ".valueColumnIndex";
  private static final String KEY_COLUMN_INDEX = MultiDatasetJaccardJob.class.getName() + ".keyColumnIndex";
  private static final String INPUT_PATHS = MultiDatasetJaccardJob.class.getName() + ".inputPaths";
  private static enum COUNT {
    NOT_COMMON_TO_ALL
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new MultiDatasetJaccardJob(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("otherDatasets", "others", "other dataset paths delimetered by ,");
    addOption("keyIndexs", "kidxs", "key indexs for group by seperated by ,", "0");
    addOption("valueIndexs", "vidxs", "value indexs for calculating jaccard.", "1");
    addOption("cleanUp", null, "true if only want _stats. otherwise false", String.valueOf(false));
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null || getOption("otherDatasets") == null) {
      return -1;
    }
    String[] inputPaths = TasteHadoopUtils.splitPrefTokens(getOption("otherDatasets"));
    List<String> totalInputPaths = new ArrayList<String>();
    totalInputPaths.add(getInputPath().toString());
    totalInputPaths.addAll(Arrays.asList(inputPaths));
    
    
    Job jaccardJob = prepareJob(getInputPath(), getOutputPath(), TextInputFormat.class, 
        MultiSetJaccardMapper.class, Text.class, Text.class, 
        MultiSetJaccardReducer.class, NullWritable.class, Text.class, TextOutputFormat.class);
    jaccardJob.setJarByClass(MultiDatasetJaccardJob.class);
    jaccardJob.getConfiguration().set(KEY_COLUMN_INDEX, getOption("keyIndexs"));
    jaccardJob.getConfiguration().set(VALUE_COLUMN_INDEX, getOption("valueIndexs"));
    jaccardJob.getConfiguration().set(INPUT_PATHS, buildInputPathString(totalInputPaths));
    
    for (String path : inputPaths) {
      FileInputFormat.addInputPath(jaccardJob, new Path(path));
    }
    jaccardJob.waitForCompletion(true);
    writeResultStat(getOutputPath("_stats"));  
    if (Boolean.parseBoolean(getOption("cleanUp")) == true) {
      HadoopClusterUtil.deletePartFiles(getConf(), getOutputPath());
    }
    return 0;
  }
  private String buildInputPathString(List<String> paths) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < paths.size(); i++) {
      if (i > 0) {
        sb.append(DELIMETER);
      }
      sb.append(paths.get(i));
    }
    return sb.toString();
  }
  private void writeResultStat(Path output) throws IOException {
    Map<Integer, Double> stats = 
        EvaluatorUtil.getResultSumPerColumns(getConf(), getOutputPath(), Arrays.asList(1), false);
    long totalCount = stats.get(EvaluatorUtil.RECORD_COUNT_SUM_INDEX).longValue();
    String outputString = totalCount + DELIMETER + (stats.get(1) / totalCount);
    HadoopClusterUtil.writeToHdfs(getConf(), output, outputString);
  }
  public static class MultiSetJaccardMapper 
    extends Mapper<LongWritable, Text, Text, Text>{
    private static int fileIndex = 0;
    private static List<Integer> keyColumnIndexs;
    private static Text outKey = new Text();
    private static Text outValue = new Text();
    
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      keyColumnIndexs = 
          OptionParseUtil.decode(context.getConfiguration().get(KEY_COLUMN_INDEX), DELIMETER);
      String[] inputPaths = TasteHadoopUtils.splitPrefTokens(context.getConfiguration().get(INPUT_PATHS));
      FileSplit split = (FileSplit)context.getInputSplit();
      Path path = split.getPath();
      for (int i = 0; i < inputPaths.length; i++) {
        if (path.toString().contains(inputPaths[i])) {
          fileIndex = i;
          break;
        }
      }
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(value.toString());
      outKey.set(OptionParseUtil.encode(tokens, keyColumnIndexs, DELIMETER));
      outValue.set(value.toString() + DELIMETER + fileIndex);
      context.write(outKey, outValue);
    }
  }
  public static class MultiSetJaccardReducer 
    extends Reducer<Text, Text, NullWritable, Text> {
    private static List<Integer> valueIndexs;
    private static Text outValue = new Text();
    private static int totalInputPathNum = 0;
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      valueIndexs = 
          OptionParseUtil.decode(context.getConfiguration().get(VALUE_COLUMN_INDEX), DELIMETER);
      String[] inputPaths = TasteHadoopUtils.splitPrefTokens(context.getConfiguration().get(INPUT_PATHS));
      totalInputPathNum = inputPaths.length;
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      long commonCount = 0;
      Map<String, Integer> occurrences = new HashMap<String, Integer>();
      Set<Integer> fileIndexs = new HashSet<Integer>();
      
      for (Text value : values) {
        String[] tokens = TasteHadoopUtils.splitPrefTokens(value.toString());
        String currentValue = OptionParseUtil.encode(tokens, valueIndexs, DELIMETER);
        int fileIndex = Integer.parseInt(tokens[tokens.length-1]);
        fileIndexs.add(fileIndex);
        if (!occurrences.containsKey(currentValue)) {
          occurrences.put(currentValue, 0);
        }
        occurrences.put(currentValue, occurrences.get(currentValue) + 1);
      }
      if (fileIndexs.size() < totalInputPathNum) {
        context.getCounter(MultiDatasetJaccardJob.COUNT.NOT_COMMON_TO_ALL).increment(1);
        return;
      }
      for (Entry<String, Integer> entry : occurrences.entrySet()) {
        if (entry.getValue() > 1) {
          commonCount++;
        }
      }
      double jaccard = 0.0;
      if (occurrences.size() > 0) {
        jaccard = commonCount / (double)occurrences.size();
      }
      outValue.set(key.toString() + DELIMETER + jaccard);
      context.write(NullWritable.get(), outValue);
    }
  }
}
