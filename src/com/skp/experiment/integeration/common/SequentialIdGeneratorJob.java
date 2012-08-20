package com.skp.experiment.integeration.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

/**
 * This class define job that assign unique sequential id from 0 ~ x
 * data flow is following.
 * 1. first mapper assign row number in this partition id and emit row number using partition id as key
 * 2. first reducer get maximum row number for partition id
 * 3. sequence file dir iterator read how many rows in each partition and calculate offset for partitions.
 * 4. second mapper load offsets per partition id and read first mapper`s intermediate file.
 *    emit row number in given partition id + offset for given partition id
 * @author doyoungYoon
 *
 */
public class SequentialIdGeneratorJob extends AbstractJob {
  //private static final Logger log = LoggerFactory.getLogger(SequentialIdGeneratorJob.class);
  public static final String RECORDS_PATH = SequentialIdGeneratorJob.class.getName() + ".recordsPath";
  public static final String SUMMARY_PATH = SequentialIdGeneratorJob.class.getName() + ".summaryPath";
  public static final String START_INDEX = SequentialIdGeneratorJob.class.getName() + ".startIndex";
  public static long totalIdCount = 0;
  
  
  private static final String DELIMETER = ",";
  public static enum COUNT {
    TOTAL_ID_COUNT
  };
  
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new SequentialIdGeneratorJob(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("cleanUp", "clean", "true if want to clean up intermediate files.", String.valueOf(true));
    addOption("startIndex", "start", "start index.", String.valueOf(0));
    
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    
    Path inputPath = getInputPath();
    Path recordsPath = getTempPath("records");
    Path summaryPath = getTempPath("summary");
    FileSystem fs = FileSystem.get(getConf());
    
    //  1. count how many records(lines) in each partition. 
    //  2. store each lines in each partition into temp files.
    //  step 2 is necessary because hadoop partition into into differenct partition id each time.
    //
    Job countJob = prepareJob(inputPath, summaryPath, TextInputFormat.class,
        CountPartitionRecordNumMapper.class, IntWritable.class, LongWritable.class,
        CountPartitionRecordNumReducer.class, IntWritable.class, LongWritable.class,
        SequenceFileOutputFormat.class);
    countJob.getConfiguration().set(RECORDS_PATH, recordsPath.toString());
    countJob.setCombinerClass(CountPartitionRecordNumReducer.class);
    countJob.waitForCompletion(true);
    
    Job generateJob = prepareJob(recordsPath, getOutputPath(), SequenceFileInputFormat.class,
        AssignRecordIdMapper.class, NullWritable.class, Text.class, 
        TextOutputFormat.class);
    generateJob.getConfiguration().set(SUMMARY_PATH, summaryPath.toString());
    generateJob.getConfiguration().setLong(START_INDEX, 
         getOption("startIndex") == null ? 0 :Long.parseLong(getOption("startIndex")));
    generateJob.waitForCompletion(true);
    // clean up
    if (getOption("cleanUp").equals("true")) {
      if (fs.exists(recordsPath)) {
        fs.delete(recordsPath, true);
      }
      if (fs.exists(summaryPath)) {
        fs.delete(summaryPath, true);
      }
      fs.deleteOnExit(getTempPath());
    }
    // record how many id has been created
    totalIdCount = generateJob.getCounters().findCounter(SequentialIdGeneratorJob.COUNT.TOTAL_ID_COUNT).getValue();
    return 0;
  }
  
  /**
   * 1. count record number per partitionID.
   * 2. store <partition_id, current partition`s line> into temp file. 
   */
  public static class CountPartitionRecordNumMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
    private static long count = 0;
    private static long curId = 0;
    private static IntWritable partitionId = new IntWritable(0);
    private static Text newKey = new Text();
    private SequenceFile.Writer writer;
    
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      int id = conf.getInt("mapred.task.partition", -1);
      partitionId.set(id);
      // get records directory
      String dir = conf.get(RECORDS_PATH);
      // set up sequence file for this partition.
      FileSystem fs = FileSystem.get(conf);
      Path writerPath = new Path(dir, String.format("records%05d", partitionId.get()));
      writer = SequenceFile.createWriter(fs, conf, writerPath, Text.class, Text.class);
      //initialize curId
      curId = 0;
    }
    
    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      IOUtils.closeStream(writer);
    }
    
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // note: if line count for this partition should be 1 based.
      // ex) if there are 10 records in this partition, count should be 10
      // but id should be 9 if we want to use 0 based id 
      context.write(partitionId, new LongWritable(++count));
      newKey.set(partitionId.get() + DELIMETER + curId++);
      //System.out.println(newKey.toString());
      writer.append(newKey, value);
    }
  }
  /**
   *  get max value for partition id
   */
  public static class CountPartitionRecordNumReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
    public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long max = 0;
      for (LongWritable val : values) {
        if (val.get() > max) max = val.get();
      }
      context.write(key, new LongWritable(max));
    }
  }
  
  
  public static class AssignRecordIdMapper extends Mapper<Text, Text, NullWritable, Text> {
    
    private static Text outValue = new Text();
    Map<Integer, Long> offsets;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Path summaryPath = new Path(context.getConfiguration().get(SUMMARY_PATH));
      long startIndex = context.getConfiguration().getLong(START_INDEX, 0);
      offsets = buildOffsets(summaryPath, startIndex);
    }
    
    private Map<Integer, Long> buildOffsets(Path input, long startIndex) throws IOException {
      Map<Integer, Long> offsets = new HashMap<Integer, Long>();
      SequenceFileDirIterator<IntWritable, LongWritable> iter = 
          new SequenceFileDirIterator<IntWritable, LongWritable>(new Path(input + "/part*"), 
              PathType.GLOB, null, null, true, new Configuration());
      long cusum = startIndex;
      while (iter.hasNext()) {
        Pair<IntWritable, LongWritable> e = iter.next();
        int partitionId = e.getFirst().get();
        long currentLineNum = e.getSecond().get();
        offsets.put(partitionId, cusum);
        cusum += currentLineNum;
      }
      return offsets;
    }
    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      // key is consist of partitionId, line number in this partition
      String[] tokens = key.toString().split(DELIMETER);
      int partitionId = Integer.parseInt(tokens[0]);
      long lineNumInPartition = Long.parseLong(tokens[1]);
      if (offsets.containsKey(partitionId)) {
        long curId = lineNumInPartition + offsets.get(partitionId);
        outValue.set(curId + DELIMETER + value);
        context.write(NullWritable.get(), outValue);
        context.getCounter(SequentialIdGeneratorJob.COUNT.TOTAL_ID_COUNT).increment(1);
      }
    }
  }
  
}
