package com.skp.experiment.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.HadoopUtil;

public class JobUtils {
  private static final int MAP_TASKS = 100000;
  private static final long LONG_TIMEOUT = 60000L * 6L;
  
  public static Job prepareReferenceWithLockMapOnlyJob(Path inputPath, Path outputPath,
      Class<? extends InputFormat> inputFormat, Class<? extends Mapper> mapper,
      Class<? extends Writable> mapperKey,
      Class<? extends Writable> mapperValue,
      Class<? extends OutputFormat> outputFormat, 
      Configuration conf) throws IOException {
    conf.setLong("mapred.task.timeout", LONG_TIMEOUT);
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setInt("mapred.map.tasks", HadoopClusterUtil.getNumberOfTaskTrackers(conf) * MAP_TASKS);
    conf.setLong("mapred.min.split.size", HadoopClusterUtil.getMinInputSplitSizeMax(conf, inputPath));
    conf.setLong("mapred.max.split.size",  HadoopClusterUtil.getMinInputSplitSizeMax(conf, inputPath));
    /*
    conf.set("mapred.child.java.opts", "-Xmx8g");
    conf.set("mapred.map.child.java.opts", "-Xmx8g");
    conf.setLong("dfs.block.size", HadoopClusterUtil.getMaxBlockSize(conf, pathToTransformed()));
    */
    Job job = HadoopUtil.prepareJob(inputPath, outputPath,
        inputFormat, mapper, mapperKey, mapperValue, outputFormat, conf);
    job.setJobName(HadoopUtil.getCustomJobName(JobUtils.class.getSimpleName(), job, mapper, Reducer.class));
    
    return job;
  }
}
