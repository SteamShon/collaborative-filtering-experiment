package com.skp.experiment.common.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import org.apache.mahout.common.AbstractJob;

import com.skp.experiment.common.OptionParseUtil;
/**
 * This class offer Repartition Join(map only), Improved Repartition Join(using secondary sort) for 
 * inner, outter, minus, gsub join options on multiple tables.
 * 1) inner: if there is same key exist in all target tables, then <src key, src value, [tgt values]> return 
 * 2) outter: if there is no same key exists in any target tables, then <src key, src values, [0]> return
 * 3) sub
 * ex)
 * hadoop jar $jar main -i data/cf/raw_input.txt -o data/cf/merged  
 * --srcKeyIndex 0,1(src tables`s key column seperated by comma) 
 * --srcValueIndex 2(src tables`s key column seperated by comma) 
 * --tgtTableOptions data/cf/meta.txt:0,1:2:filter;data/cf/meta2.txt:1,0:2:filter
 * (tgt table`s name:tgt table`s key columns:tgt table`s value columns:joinType)
 *
 */
public class ImprovedRepartitionJoinAndFilterJob extends AbstractJob {
  public static final String SRC_TABLE = ImprovedRepartitionJoinAndFilterJob.class.getName() + ".srcTable";
  public static final String SRC_KEY_INDEX = ImprovedRepartitionJoinAndFilterJob.class.getName() + ".srcKeyIndex";
  public static final String SRC_VALUE_INDEX = ImprovedRepartitionJoinAndFilterJob.class.getName() + ".srcValueIndex";
  public static final String TGT_TABLE_OPTIONS = ImprovedRepartitionJoinAndFilterJob.class.getName() + ".tgtTableOptions";
  public static final String REPARTITION_JOIN = ImprovedRepartitionJoinAndFilterJob.class.getName() + ".repartitionJoin";
  public static final String DEFAULT_VALUE = ImprovedRepartitionJoinAndFilterJob.class.getName() + ".defaultValue";
  public static final String DELIMETER = ",";
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ImprovedRepartitionJoinAndFilterJob(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("srcKeyIndex", "sidx", "src key index, seperated by comma");
    addOption("tgtTableOptions", "tgt", "list of option for tgt table {(tablename:keyindexs:valueindexs)}");
    addOption("mapOnly", null, "true if only repartition join needs to be used.");
    addOption("defaultValue", null, "default value for outer join", "null");
    if (parseArguments(args) == null) {
      return -1;
    }
    boolean repartitionOnly = 
        getOption("mapOnly") != null && getOption("mapOnly").equals("true") ? true : false;
    
    Job joinJob = null;
    
    if (repartitionOnly) {
      /**map only job */
      joinJob = prepareJob(getInputPath(), getOutputPath(), TextInputFormat.class,
          RepartitionJoinMapper.class, NullWritable.class, Text.class,
          TextOutputFormat.class);
    } else {
      /** improved repartition join:
       *  reference tables comes in reducer first, and src table later.
       *  using secondary sort.
       * */
      joinJob = prepareJob(getInputPath(), getOutputPath(), TextInputFormat.class,
          ImprovedRepartitionJoinMapper.class, CompositeJoinKey.class, CompositeJoinValue.class, 
          ImprovedRepartitionJoinReducer.class, NullWritable.class, Text.class, 
          TextOutputFormat.class);
      
      /** secondary sort setup */
      joinJob.setPartitionerClass(CompositeJoinKeyPartitioner.class);
      joinJob.setSortComparatorClass(CompositeJoinKeyComparator.class);
      joinJob.setGroupingComparatorClass(CompositeJoinKeyGroupingComparator.class);
    }
    joinJob.getConfiguration().set(SRC_TABLE, getInputPath().toString());
    joinJob.getConfiguration().set(SRC_KEY_INDEX, getOption("srcKeyIndex"));
    joinJob.getConfiguration().set(TGT_TABLE_OPTIONS, getOption("tgtTableOptions"));
    joinJob.getConfiguration().set(DEFAULT_VALUE, getOption("defaultValue"));
    // iterate all target tables and add them into input paths
    List<JoinOption> tgtTableOptions = JoinOptionUtils.parseOptionStrings(getOption("tgtTableOptions"));
    if (!repartitionOnly) {
      for (JoinOption option : tgtTableOptions) {
        FileInputFormat.addInputPath(joinJob, new Path(option.getTable()));
      }
    }
    joinJob.waitForCompletion(true);
    
    return 0;
  }
  private static List<Integer> getAllColumnIndexs(String[] tokens) {
    List<Integer> ret = new ArrayList<Integer>();
    for (int i = 0; i < tokens.length; i++) {
      ret.add(i);
    }
    return ret;
  }
  private static String joinTokens(String[] tokens) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < tokens.length; i++) {
      if (i != 0) {
        sb.append(DELIMETER);
      }
      sb.append(tokens[i]);
    }
    return sb.toString();
  }
  public static class ImprovedRepartitionJoinMapper extends 
    Mapper<LongWritable, Text, CompositeJoinKey, CompositeJoinValue> {
    
    private static String srcTableName;
    private static List<Integer> srcKeyIndexs;
    private static List<Integer> srcValueIndexs;
    private static List<JoinOption> tgtOptions;
    
    private static int joinOptionIndex;
    private static CompositeJoinKey outKey = new CompositeJoinKey();
    private static CompositeJoinValue outValue = new CompositeJoinValue();
    
    @Override
    protected void setup(Context ctx) throws IOException,
        InterruptedException {
      // get parameter about source tables
      srcTableName = ctx.getConfiguration().get(SRC_TABLE);
      srcKeyIndexs = OptionParseUtil.decode(ctx.getConfiguration().get(SRC_KEY_INDEX), JoinOption.INNER_DELIMETER);
      //srcValueIndexs = OptionParseUtil.decode(ctx.getConfiguration().get(SRC_VALUE_INDEX), JoinOption.INNER_DELIMETER);
      // parse options for target tables
      tgtOptions = JoinOptionUtils.parseOptionStrings(ctx.getConfiguration().get(TGT_TABLE_OPTIONS));
      // note that suffix for tables are define following order.
      // src table = n, tgt table 0 = 0, tgt table 1 = 1, .... tgt table n -1 = n -1 
      joinOptionIndex = tgtOptions.size();
      FileSplit split = (FileSplit)ctx.getInputSplit();
      Path path = split.getPath();
     
      for (int i = 0; i < tgtOptions.size(); i++) {
        if (path.toString().contains(tgtOptions.get(i).getTable())) {
          joinOptionIndex = i;
          break;
        }
      }
    }
    
    @Override
    protected void map(LongWritable offset, Text line, Context ctx) 
      throws IOException, InterruptedException {
      String[] tokens = JoinOptionUtils.splitPrefTokens(line.toString());
      if (srcValueIndexs == null) {
        srcValueIndexs = getAllColumnIndexs(tokens);
      }
      JoinOption curOption = new JoinOption();
      if (joinOptionIndex == tgtOptions.size()) {
        // src table setup.
        curOption.setTable(srcTableName);
        curOption.setTargetTableKeyIndexs(srcKeyIndexs);
        curOption.setTargetTableValueIndexs(srcValueIndexs);
        curOption.setType("inner");
      } else {
        // target table setup
        curOption.setTable(tgtOptions.get(joinOptionIndex).getTable());
        curOption.setTargetTableKeyIndexs(tgtOptions.get(joinOptionIndex).getTargetTableKeyIndexs());
        curOption.setTargetTableValueIndexs(tgtOptions.get(joinOptionIndex).getTargetTableValueIndexs());
        curOption.setType(tgtOptions.get(joinOptionIndex).getType());
      }
      
      String keyStr = JoinOptionUtils.fetchFileds(tokens, curOption.getTargetTableKeyIndexs());
      String valueStr = JoinOptionUtils.fetchFileds(tokens, curOption.getTargetTableValueIndexs());
      
      int suffix = joinOptionIndex;
      outKey.set(keyStr, suffix);
      outValue.set(valueStr, suffix);
     
      ctx.write(outKey, outValue);
    }
  }
  public static class ImprovedRepartitionJoinReducer extends 
    Reducer<CompositeJoinKey, CompositeJoinValue, NullWritable, Text> {
    private static Text outValue = new Text();
    private static List<JoinOption> tgtOptions;
    private static String defaultValue = "";
    @Override
    protected void setup(Context ctx)
        throws IOException, InterruptedException {
      
      tgtOptions = JoinOptionUtils.parseOptionStrings(ctx.getConfiguration().get(TGT_TABLE_OPTIONS));
      defaultValue = ctx.getConfiguration().get(DEFAULT_VALUE);
    }
    
    @Override
    protected void reduce(CompositeJoinKey key, Iterable<CompositeJoinValue> values, Context ctx) 
        throws IOException,
        InterruptedException {
      // following code only works when secondary sort is properly used.
      // assumes that target tables appear first before source tables.
      Map<Integer, List<String>> tgtTableValues = new HashMap<Integer, List<String>>();
      for (int i = 0; i < tgtOptions.size(); i++) {
        tgtTableValues.put(i, new ArrayList<String>());
      }
      
      for (CompositeJoinValue value : values) {
        int suffix = value.getSuffix();
        boolean isSrcTable = suffix == tgtOptions.size() ? true : false;
        if (!isSrcTable) {
          tgtTableValues.get(value.getSuffix()).add(value.getValue());
        } else {
          mergeOutput(ctx, key, value, tgtOptions, tgtTableValues);
        }
      }
    }
    private void mergeOutput(Context ctx, CompositeJoinKey key, CompositeJoinValue value, 
        List<JoinOption> tgtOptions, Map<Integer, List<String>> tgtTableValues) 
            throws IOException, InterruptedException {
      boolean skipThisValue = false;
      String[] valueTokens = value.getValue().split(DELIMETER);
      StringBuffer sb = new StringBuffer();
      
      
      for (int i = 0; i < tgtOptions.size(); i++) {
        JoinOption option = tgtOptions.get(i);
        List<String> curTgtTableValues = tgtTableValues.get(i);
        // check when any target table has filter option and values for this key
        if (option.getType().equals("filter") && curTgtTableValues.size() > 0) {
          skipThisValue = true;
          break;
        }
        if (option.getType().equals("inner")) {
          // option is inner but there is no match in this target table for this key
          if (curTgtTableValues.size() == 0) {
            skipThisValue = true;
            break;
          } else {
            // inner join
            sb.append(JoinOptionUtils.DELIMETER).append(curTgtTableValues.get(0));
          }
        }
        if (option.getType().equals("outer")) {
          sb.append(JoinOptionUtils.DELIMETER).append(
              curTgtTableValues.size() > 0 ? curTgtTableValues.get(0) : defaultValue);
        }
        if (option.getType().equals("sub")) {
          if (curTgtTableValues.size() == 0) {
            skipThisValue = true;
            break;
          } else {
            for (int j = 0; j < option.getSourceTableKeyIndexs().size(); j++) {
              int srcIdx = option.getSourceTableKeyIndexs().get(j);
              if (srcIdx >= 0 && srcIdx < valueTokens.length) {
                valueTokens[srcIdx] = curTgtTableValues.get(j);
              }
            }
          }
        }
      }
      
      if (!skipThisValue) {
        String necessaryStr = joinTokens(valueTokens);
        String extraStr = sb.toString();
        if (extraStr.length() > 0) {
          outValue.set(necessaryStr + extraStr);
        } else {
          outValue.set(necessaryStr);
        }
        
        ctx.write(NullWritable.get(), outValue);
      }
    }
  }
  
  public static class RepartitionJoinMapper extends 
    Mapper<LongWritable, Text, NullWritable, Text> {
    
    private static List<Integer> srcKeyIndexs;
    private static List<Integer> srcValueIndexs;
    
    private static List<JoinOption> tgtOptions;
    private static List<Map<String, String>> tgtTableCaches;
    private static Text outValue = new Text();
    private static String defaultValue = "null";
    
    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      //srcTableName = ctx.getConfiguration().get(SRC_TABLE);
      srcKeyIndexs = OptionParseUtil.decode(ctx.getConfiguration().get(SRC_KEY_INDEX), JoinOption.INNER_DELIMETER);
      //srcValueIndexs = OptionParseUtil.decode(ctx.getConfiguration().get(SRC_VALUE_INDEX), JoinOption.INNER_DELIMETER);
      
      tgtOptions = JoinOptionUtils.parseOptionStrings(ctx.getConfiguration().get(TGT_TABLE_OPTIONS));
      defaultValue = ctx.getConfiguration().get(DEFAULT_VALUE);
      tgtTableCaches = new ArrayList<Map<String, String>>();
      
      for (int i = 0; i < tgtOptions.size(); i++) {
        Path curPath = new Path(tgtOptions.get(i).getTable());
        Map<String, String> cache = JoinUtils.fetchTextFiles(ctx, 
            curPath, DELIMETER, tgtOptions.get(i).getTargetTableKeyIndexs(), 
            tgtOptions.get(i).getTargetTableValueIndexs());
        tgtTableCaches.add(cache);
      }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(LongWritable key, Text value, Context ctx) 
        throws IOException, InterruptedException {
      String[] tokens = JoinOptionUtils.splitPrefTokens(value.toString());
      if (srcValueIndexs == null) {
        srcValueIndexs = getAllColumnIndexs(tokens);
      }
      String srcTableKey = JoinOptionUtils.fetchFileds(tokens, srcKeyIndexs);
      String srcTableValue = JoinOptionUtils.fetchFileds(tokens, srcValueIndexs);
      boolean skipThisValue = false;
      //System.err.println("key: " + key.toString() + "\tvalue: " + value.toString());
      StringBuffer sb = new StringBuffer();
      //sb.append(value.toString());
      
      for (int i = 0; i < tgtOptions.size(); i++) {
        JoinOption option = tgtOptions.get(i);
        Map<String, String> curTgtTableCache = tgtTableCaches.get(i);
        
        if (option.getType().equals("filter") && curTgtTableCache.containsKey(srcTableKey)) {
          skipThisValue = true;
          break;
        }
        if (option.getType().equals("inner")) {
          if (!curTgtTableCache.containsKey(srcTableKey)) {
            skipThisValue = true;
            break;
          }
          sb.append(JoinOptionUtils.DELIMETER).append(curTgtTableCache.get(srcTableKey));
        }
        if (option.getType().equals("outer")) {
          sb.append(JoinOptionUtils.DELIMETER).append(
              curTgtTableCache.containsKey(srcTableKey) ? curTgtTableCache.get(srcTableKey) : defaultValue);
        }
        if (option.getType().equals("sub")) {
          if (!curTgtTableCache.containsKey(srcTableKey)) {
            skipThisValue = true;
            break;
          }
          String[] tgtTableValues = curTgtTableCache.get(srcTableKey).split(DELIMETER);
          for (int j = 0; j < option.getSourceTableKeyIndexs().size(); j++) {
            int srcIdx = option.getSourceTableKeyIndexs().get(j);
            if (srcIdx >= 0 && srcIdx < tokens.length) {
              tokens[srcIdx] = tgtTableValues[j];
            }
          }
        }
      }
      //System.err.println("extra: " + sb.toString());
      if (!skipThisValue) {
        String neccessary = joinTokens(tokens);
        String extra = sb.toString();
        if (extra.length() > 0) {
          outValue.set(neccessary + extra);
        } else {
          outValue.set(neccessary);
        }
        //System.err.println("output: " + outValue.toString());
        //outValue.set(sb.toString());
        ctx.write(NullWritable.get(), outValue);
      }
    }  
  }
}



