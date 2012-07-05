package com.skp.experiment.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.skp.experiment.cf.als.hadoop.ParallelALSFactorizationJob;


public class HadoopClusterUtil {
  private static final Logger log = LoggerFactory.getLogger(HadoopClusterUtil.class);
  public static int CLUSTER_SIZE_DEV = 10;
  public static int CLUSTER_SIZE_PROD = 30;
  public static int MAP_TASKS_PER_NODE = 7;
  public static int REDUCER_TASKS_PER_NODE = 7;
  public static long DEFALUT_INPUT_SPLIT_SIZE = 1024L * 1024L * 64L;
  public static long PHYSICAL_MEMERY_LIMIT = 14L * 1024L * 1024L * 1024L;
  
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
}
