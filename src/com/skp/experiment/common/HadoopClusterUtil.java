package com.skp.experiment.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.skp.experiment.common.parameter.DefaultOptionCreator;
/**
 * helper util regarding to HDFS
 * @author doyoung
 *
 */

public class HadoopClusterUtil {
  private static final Logger log = LoggerFactory.getLogger(HadoopClusterUtil.class);
  public static int CLUSTER_SIZE_DEV = 10;
  public static int CLUSTER_SIZE_PROD = 30;
  public static int MAP_TASKS_PER_NODE = 7;
  public static int REDUCER_TASKS_PER_NODE = 7;
  public static long DEFALUT_INPUT_SPLIT_SIZE = 1024L * 1024L * 64L;
  public static long PHYSICAL_MEMERY_LIMIT = 14L * 1024L * 1024L * 1024L;
  public static long DEFAULT_HEAP_SIZE = 2L * 1024L * 1024L * 1024L;
  
  @SuppressWarnings("deprecation")
  public static ClusterStatus getClusterStatus(Configuration conf) throws IOException {
    return new JobClient(new JobConf(conf, HadoopClusterUtil.class)).getClusterStatus();
  }

  public static int getNumberOfTaskTrackers(Configuration conf) throws IOException {
    int numTaskTrackers = getClusterStatus(conf).getTaskTrackers();
    log.info("Task Trackers Num: {}", numTaskTrackers);
    return numTaskTrackers;
  }
  
  public static int getMaxMapTasks(Configuration conf) throws IOException {
    int maxMapTasks = getClusterStatus(conf).getMaxMapTasks();
    log.info("Max Map Task On This Cluster: {}", maxMapTasks);
    return maxMapTasks;
  }
  
  public static long getHdfsPathSize(Path input) throws IOException {
    long size = getHdfsPathSize(new Configuration(), input);
    log.info("HDFS Path Size: {}\t{}", input.toString(), size);
    return size;
  }
  
  public static long getHdfsPathSize(Configuration conf, Path input) throws IOException  {
    long size = FileSystem.get(conf).getContentSummary(input).getLength();
    log.info("HDFS Path Size: {}\t{}", input.toString(), size);
    return size;
  }
  public static long getMinInputSplitSizeMax(Configuration conf, Path input) throws IOException {
    long pathSize = HadoopClusterUtil.getHdfsPathSize(conf, input);
    long minSplitSize = (long) Math.ceil(pathSize / (double)HadoopClusterUtil.getMaxMapTasks(conf));
    log.info("HDFS Min Split Size: {}\t{}", input.toString(), minSplitSize);
    return minSplitSize;
  }
  public static long getMinInputSplitSizeMin(Configuration conf, Path input) throws IOException {
    int taskTrackerNums = HadoopClusterUtil.getNumberOfTaskTrackers(conf);
    long pathSize = HadoopClusterUtil.getHdfsPathSize(conf, input);
    long minSplitSize = (long) Math.ceil(pathSize / (double)taskTrackerNums);
    log.info("HDFS Min Split Size: {}\t{}", input.toString(), minSplitSize);
    return minSplitSize;
  }
  public static long getMaxBlockSize(Configuration conf, Path input) throws IOException {
    long blockSize = 1024 * 1024 * 64;
    long inputSize = getHdfsPathSize(conf, input);
    int taskTrackerNum = getNumberOfTaskTrackers(conf);
    
    for (int pow = 1; pow < 40; pow ++) {
      long currentBlockSize = (long)Math.pow(2, pow);
      if (currentBlockSize * taskTrackerNum >= inputSize) {
        blockSize = currentBlockSize;
        break;
      }
    }
    log.info("BlockSize: {}\t{}", input.toString(), blockSize);
    return blockSize;
  }
  public static void deletePartFiles(Configuration conf, Path dir) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.globStatus(new Path(dir.toString() + "/part*"));
    for (FileStatus file : files) {
      fs.delete(file.getPath(), true);
    }
  }
  public static void writeToHdfs(Configuration conf, Path output, String outputString) 
      throws IOException {
    writeToHdfs(conf, output, outputString, true);
  }
  public static void writeToHdfs(Configuration conf, Path output, String outputString, boolean newline) 
      throws IOException {
    FSDataOutputStream out = null;
    try {
      FileSystem fs = FileSystem.get(conf);
      out = fs.create(fs.makeQualified(output));
      if (newline) {
        out.writeBytes(outputString + DefaultOptionCreator.NEWLINE);
      } else {
        out.writeBytes(outputString);
      }
    } finally {
      if (out != null) {
        out.close();
      }
    } 
  }
  public static void renamePath(Configuration conf, Path input, Path output) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    fs.rename(input, output);
  }
  public static String getAttemptId(Configuration conf) throws IllegalArgumentException
  {
      if (conf == null) {
          throw new NullPointerException("conf is null");
      }

      String taskId = conf.get("mapred.task.id");
      if (taskId == null) {
          throw new IllegalArgumentException("Configutaion does not contain the property mapred.task.id");
      }

      String[] parts = taskId.split("_");
      if (parts.length != 6 ||
          !parts[0].equals("attempt") ||
          (!"m".equals(parts[3]) && !"r".equals(parts[3]))) {
        throw new IllegalArgumentException("TaskAttemptId string : " + taskId + " is not properly formed");
      }
      
      return "part-" + parts[3] + "-" + parts[4];
  }
}
