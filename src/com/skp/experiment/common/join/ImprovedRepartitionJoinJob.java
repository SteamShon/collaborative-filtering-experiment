package com.skp.experiment.common.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.AbstractJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.skp.experiment.cf.als.hadoop.ParallelALSFactorizationJob;
import com.skp.experiment.common.OptionParseUtil;

public class ImprovedRepartitionJoinJob extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(ImprovedRepartitionJoinJob.class);

  private static final String SRC_TABLE = ImprovedRepartitionJoinJob.class.getName() + ".srcTable";
  private static final String TGT_TABLE = ImprovedRepartitionJoinJob.class.getName() + ".tgtTable";
  private static final String SRC_KEY_INDEX = ImprovedRepartitionJoinJob.class.getName() + ".srcKeyIndex";
  private static final String TGT_KEY_INDEX = ImprovedRepartitionJoinJob.class.getName() + ".tgtKeyIndex";
  private static final String SRC_VALUE_INDEX = ImprovedRepartitionJoinJob.class.getName() + ".srcValueIndex";
  private static final String TGT_VALUE_INDEX = ImprovedRepartitionJoinJob.class.getName() + ".tgtValueIndex";
  private static final String JOIN_TYPE = ImprovedRepartitionJoinJob.class.getName() + ".joinType";
  private static final String DELIMETER = ",";
  
  private static int SRC_TABLE_SUFFIX = 1;
  private static int TGT_TABLE_SUFFIX = 0;
  private static String nullStr = "0";
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ImprovedRepartitionJoinJob(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("targetTable", "tgt", "path to target table.");
    addOption("srcKeyIndex", "sidx", "src key index, seperated by comma");
    addOption("srcValueIndex", "svalues", "src values index");
    addOption("tgtKeyIndex", "tidx", "tgt key index");
    addOption("tgtValueIndex", "tvalues", "tgt values index");
    addOption("delimeter", "d", "delimeter", ",");
    addOption("joinType", "type", "join type{inner(default), outter}", "inner");
    
    if (parseArguments(args) == null) {
      return -1;
    }
    Job joinJob = prepareJob(getInputPath(), getOutputPath(), TextInputFormat.class,
        ImprovedRepartitionJoinMapper.class, CompositeJoinKey.class, CompositeJoinValue.class, 
        ImprovedRepartitionJoinReducer.class, NullWritable.class, Text.class, 
        TextOutputFormat.class);
    joinJob.getConfiguration().set(SRC_TABLE, getInputPath().toString());
    joinJob.getConfiguration().set(SRC_KEY_INDEX, getOption("srcKeyIndex"));
    joinJob.getConfiguration().set(SRC_VALUE_INDEX, getOption("srcValueIndex"));
    joinJob.getConfiguration().set(TGT_KEY_INDEX, getOption("tgtKeyIndex"));
    joinJob.getConfiguration().set(TGT_VALUE_INDEX, getOption("tgtValueIndex"));
    joinJob.getConfiguration().set(JOIN_TYPE, getOption("joinType"));
    
    FileInputFormat.addInputPath(joinJob, new Path(getOption("targetTable")));
    
    joinJob.setPartitionerClass(CompositeJoinKeyPartitioner.class);
    joinJob.setSortComparatorClass(CompositeJoinKeyComparator.class);
    joinJob.setGroupingComparatorClass(CompositeJoinKeyGroupingComparator.class);
    joinJob.waitForCompletion(true);
    
    return 0;
  }
  
  public static class ImprovedRepartitionJoinMapper extends 
    Mapper<LongWritable, Text, CompositeJoinKey, CompositeJoinValue> {
    private static boolean isSrcTable = false;
    private static List<Integer> srcKeyIndexs;
    private static List<Integer> srcValueIndexs;
    private static List<Integer> tgtKeyIndexs;
    private static List<Integer> tgtValueIndexs;
    private static CompositeJoinKey outKey = new CompositeJoinKey();
    private static CompositeJoinValue outValue = new CompositeJoinValue();
    
    @Override
    protected void setup(Context ctx) throws IOException,
        InterruptedException {
      String srcTableName = ctx.getConfiguration().get(SRC_TABLE);
      srcKeyIndexs = OptionParseUtil.decode(ctx.getConfiguration().get(SRC_KEY_INDEX), DELIMETER);
      srcValueIndexs = OptionParseUtil.decode(ctx.getConfiguration().get(SRC_VALUE_INDEX), DELIMETER);
      tgtKeyIndexs = OptionParseUtil.decode(ctx.getConfiguration().get(TGT_KEY_INDEX), DELIMETER);
      tgtValueIndexs = OptionParseUtil.decode(ctx.getConfiguration().get(TGT_VALUE_INDEX), DELIMETER);
      
      FileSplit split = (FileSplit)ctx.getInputSplit();
      Path path = split.getPath();
      //System.out.println(path.toString() + "\t" + srcTableName);
      if (path.toString().contains(srcTableName)) {
        isSrcTable = true;
      } else {
        isSrcTable = false;
      }
    }
    @Override
    protected void map(LongWritable offset, Text line, Context ctx) 
      throws IOException, InterruptedException {
      String[] tokens = TasteHadoopUtils.splitPrefTokens(line.toString());
      List<Integer> keyIndexs = isSrcTable ? srcKeyIndexs : tgtKeyIndexs;
      List<Integer> valueIndexs = isSrcTable ? srcValueIndexs : tgtValueIndexs;
      int suffix = isSrcTable ? SRC_TABLE_SUFFIX : TGT_TABLE_SUFFIX;
      outKey.set(OptionParseUtil.encode(tokens, keyIndexs, DELIMETER), suffix);
      outValue.set(OptionParseUtil.encode(tokens, valueIndexs, DELIMETER), suffix);
      if (outKey.getJoinKey() != null && outValue.getValue() != null) {
    	  ctx.write(outKey, outValue);
      } else {
    	  log.info("MAP:\t" + outKey.getJoinKey() + "\t" + outKey.getSuffix() + "\t" + outValue.getValue());
      }
      //System.out.println("MAP:\t" + outKey.getJoinKey() + "," + outKey.getSuffix() + "," + outValue.getValue());
      //ctx.write(outKey, outValue);
    }
  }
  public static class ImprovedRepartitionJoinReducer extends 
    Reducer<CompositeJoinKey, CompositeJoinValue, NullWritable, Text> {
    
    private static String joinType;
    private static Text outValue = new Text();
    
    @Override
    protected void setup(Context ctx)
        throws IOException, InterruptedException {
      joinType = ctx.getConfiguration().get(JOIN_TYPE).toLowerCase();
    }
    
    @Override
    protected void reduce(CompositeJoinKey key, Iterable<CompositeJoinValue> values, Context ctx) 
        throws IOException,
        InterruptedException {
      List<String> tgtTableValues = new ArrayList<String>();
      for (CompositeJoinValue value : values) {
        //System.out.println("REDUCE: " + key.getJoinKey() + "\t" + value.getValue() + "," + value.getSuffix());
        if (value.getSuffix() == TGT_TABLE_SUFFIX) {
          tgtTableValues.add(value.getValue());
        } else {
          int matchedCount = 0;
          for (String tgtValue : tgtTableValues) {
            if (joinType.equals("minus") == false) {
              outValue.set(key.getJoinKey() + DELIMETER + value.getValue() + DELIMETER + tgtValue);
              ctx.write(NullWritable.get(), outValue);
            }
            matchedCount++;
          }
          if (matchedCount == 0 && (joinType.equals("outter") || joinType.equals("minus"))) {
            if (joinType.equals("outter")) {
              outValue.set(key.getJoinKey() + DELIMETER + value.getValue() + DELIMETER + nullStr);
            } else if (joinType.equals("minus")) {
              outValue.set(key.getJoinKey() + DELIMETER + value.getValue());
            }
            ctx.write(NullWritable.get(), outValue);
          }
        }
      }
    }
  }
}
