package com.skp.experiment.cf.als.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.iterator.FileLineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.skp.experiment.cf.evaluate.hadoop.EvaluatorUtil;
import com.skp.experiment.common.DistinctColumnValuesJob;
import com.skp.experiment.common.OptionParseUtil;
import com.skp.experiment.common.join.ImprovedRepartitionJoinAndFilterJob;
import com.skp.experiment.common.join.JoinOptionUtils;
import com.skp.experiment.common.mapreduce.IdentityMapper;
import com.skp.experiment.integeration.common.SequentialIdGeneratorJob;

public class Conv2IndexJob extends AbstractJob {
  //public static Map<Integer, Long> indexSizes = new HashMap<Integer, Long>();
  private static final Logger log = LoggerFactory.getLogger(Conv2IndexJob.class);
  private static final String DELIMETER = ",";
  private boolean oldIndexExist = false;
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Conv2IndexJob(), args);
  }
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("columnIndexs", "cidxs", "column indexs to convert into integer index");
    addOption("mapOnlyColumnIndexs", "mcidxs", "column indexs to load into memory.");
    
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    //oldIndexExist = checkIndexExist();
    
    Path distinctPath = getTempPath("distinct");
    String columnIndexs = getOption("columnIndexs");
    String mapOnlyColumnIndexs = getOption("mapOnlyColumnIndexs");
    List<Integer> cidxs = OptionParseUtil.decode(columnIndexs, JoinOptionUtils.DELIMETER);
    List<Integer> mapOnlyCidxs = new ArrayList<Integer>();
    mapOnlyCidxs = OptionParseUtil.decode(mapOnlyColumnIndexs, JoinOptionUtils.DELIMETER);
    
    // step 1. get distinct values for columnIndexs
    Map<Integer, Long> totalIndexSizes = buildIndex(getInputPath(), columnIndexs, cidxs, getOutputPath());
    log.info("Writting out: {}", pathToIndexSize("new").toString());
    writeIndexSizesIntoHdfs(pathToIndexSize("new"), totalIndexSizes); 
    
    // step 3. substitute column values with indexs
    Path tgtPath = getInputPath();
    String[] jobArgs = null;
    for (Integer cidx : cidxs) {
      if (mapOnlyCidxs.contains(cidx)) {
        jobArgs = new String[]{
            "-i", tgtPath.toString(), "-o", new Path(distinctPath, cidx + "_append").toString(), 
            "-sidx", String.valueOf(cidx), "-tgt", 
            pathToIndexOutput(cidx, true).toString() + ":" + cidx + ":1:0:sub", "--mapOnly", "true"
        };
      } else {
        jobArgs = new String[]{
            "-i", tgtPath.toString(), "-o", new Path(distinctPath, cidx + "_append").toString(), 
            "-sidx", String.valueOf(cidx), "-tgt", 
            pathToIndexOutput(cidx, true).toString() + ":" + cidx + ":1:0:sub"
        };
      }
       
      ToolRunner.run(new ImprovedRepartitionJoinAndFilterJob(), jobArgs);
      tgtPath = new Path(distinctPath, cidx + "_append");
    }
    
    EvaluatorUtil.renamePath(getConf(), tgtPath, getOutputPath());
    return 0;
  }
  private boolean checkIndexExist() throws IOException {
    boolean exist = false;
    Path oldIndex = pathToIndex("new");
    Path oldIndexSize = pathToIndexSize("new");
    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(oldIndex) && fs.exists(oldIndexSize)) {
      log.info("Move {} --> {}", pathToIndex("new").toString(), pathToIndex("old").toString());
      fs.rename(pathToIndex("new"), pathToIndex("old"));
      log.info("Move {} --> {}", pathToIndexSize("new").toString(), pathToIndexSize("old").toString());
      fs.rename(pathToIndexSize("new"), pathToIndex("old"));
      exist = true;
    } 
    return exist;
  }
  /*
   * case1: no reference exist
   * 1. build distinct values into "output/0, output/1"...
   * 2. build index file into output_index_new/0, output_index_new/1....
   * 3. store each index`s dimension(max_id + 1) into indexSizes
   * case2: there are old indexs exist
   * 1. get distinct values into "output_all/0, output_all/1" ...
   * 2. get current distinct values - old index file. store this into output_all/0_minus, output_all/1_minus
   * 3. get old index`s size per column
   * 4. generate sequential id per output_all/0_minus, output_all/1_minus start from old index`s dimension
   * store this into  output_all/0_minus_index, output_all/1_minus_index
   * 5. merge output_all/0_minus_index and old_index_path/0
   */
  private Map<Integer, Long> buildIndex(Path input, String columnIndexs, List<Integer> cidxs, Path output) 
      throws Exception {
    Map<Integer, Long> totalIndexSizes = new HashMap<Integer, Long>();
    // step 1. get distinct values for columnIndexs
    // create output/0/.... output/1/....
    Path distinctValueParentPath = 
        oldIndexExist == false ? getTempPath("distinct") : getTempPath("distinct_all");
        
    ToolRunner.run(new DistinctColumnValuesJob(), new String[]{
      "-i", getInputPath().toString(), "-o", distinctValueParentPath.toString(), 
      "--columnIndexs", columnIndexs
    });
    
    for (Integer cidx : cidxs) {
      if (!oldIndexExist) {
        ToolRunner.run(new SequentialIdGeneratorJob(), new String[] {
          "-i", pathToDistinctValues(distinctValueParentPath, cidx).toString(), 
          "-o", pathToIndexOutput(cidx, true).toString(),
          "--tempDir", getTempPath("idgenerate" + cidx).toString(),
          "--cleanUp", "false"
        });
        totalIndexSizes.put(cidx, SequentialIdGeneratorJob.totalIdCount);
      } else {
        // get new commers only
        ToolRunner.run(new ImprovedRepartitionJoinAndFilterJob(), new String[]{
          "-i", pathToDistinctValues(distinctValueParentPath, cidx).toString(), 
          "-o", pathToDistinctValues(distinctValueParentPath, cidx).toString()  + "_filtered",
          "-sidx", "0", 
          "-tgt", pathToIndex("old") + "/" + cidx + ":0:1:0:filter"
        });
        
        // we need to calculate old index size to append
        FileLineIterator iter = new FileLineIterator(FileSystem.get(getConf()).open(pathToIndexSize("old")));
        while (iter.hasNext()) {
          String[] tokens = iter.next().split(DELIMETER);
          totalIndexSizes.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
          //System.out.println("Old: " + tokens[0] + "\tSize: " + tokens[1]);
        }
        ToolRunner.run(new SequentialIdGeneratorJob(), new String[]{
          "-i", pathToDistinctValues(distinctValueParentPath, cidx).toString() + "_filtered",
          "-o",  pathToDistinctValues(distinctValueParentPath, cidx).toString() + "_filtered_index",
          "--tempDir", getTempPath("idgenerate2" + cidx).toString(),
          "--cleanUp", "false",
          "--startIndex", String.valueOf(totalIndexSizes.get(cidx))
        });
        //System.out.println("New: " + cidx + "\tSize: "+ SequentialIdGeneratorJob.totalIdCount);
        totalIndexSizes.put(cidx,  totalIndexSizes.get(cidx) + SequentialIdGeneratorJob.totalIdCount);
        //System.out.println("Total: " + cidx + "\tSize: " + totalIndexSizes.get(cidx));
        mergeTwoPath(new Path(pathToIndex("old"), String.valueOf(cidx)),
            new Path(pathToDistinctValues(distinctValueParentPath, cidx).toString() + "_filtered_index"),
            pathToIndexOutput(cidx, true));
      }
    }
    return totalIndexSizes;
  }
  
  private void writeIndexSizesIntoHdfs(Path output, Map<Integer, Long> indexSizes) 
      throws IOException {
    StringBuffer sb = new StringBuffer();
    for (Entry<Integer, Long> idxSize : indexSizes.entrySet()) {
      //System.out.println("writting out\t" + idxSize.getKey() + ", " + idxSize.getValue());
      sb.append(idxSize.getKey() + DELIMETER + idxSize.getValue() + EvaluatorUtil.NEWLINE);
    }
    EvaluatorUtil.writeToHdfs(getConf(), output, sb.toString(), false);
  }
  
  //hadoop 0.20.2 doens`t support append mode
  private void mergeTwoPath(Path inputA, Path inputB, Path output) 
      throws IOException, InterruptedException, ClassNotFoundException {
    Job mergeJob = prepareJob(inputA, output, TextInputFormat.class, 
        IdentityMapper.class, NullWritable.class, Text.class, 
        TextOutputFormat.class);
    FileInputFormat.addInputPath(mergeJob, inputB);
    mergeJob.getConfiguration().setBoolean(IdentityMapper.VALUE_ONLY_OUT, true);
    mergeJob.waitForCompletion(true);
  }
  
  private Path pathToIndexSize(String oldOrNew) {
    return new Path(getOutputPath().toString() + "_index_" + oldOrNew + "_size");
  }
  private Path pathToIndex(String oldOrNew) {
    return new Path(getOutputPath().toString() + "_index_" + oldOrNew);
  }
  private Path pathToDistinctValues(Path basePath, int columnIndex) {
    return new Path(basePath, String.valueOf(columnIndex));
  }
  private Path pathToIndexOutput(int columnIndex, boolean isNew) {
    return new Path(getOutputPath().toString() + (isNew ? "_index_new" : "_index_old"), 
        String.valueOf(columnIndex));
  }
}

