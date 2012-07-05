package com.skp.experiment.cf.als.hadoop;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.math.VectorWritable;

import com.skp.experiment.common.mapreduce.IdentityMapper;
import com.skp.experiment.common.mapreduce.IdentityReducer;
import com.skp.experiment.common.mapreduce.MapFileOutputFormat;

public class CreateMapFileFromSeq {
  private static Integer indexInterval = 1;
  
  /* get input path directory name and merge all sub files and make it as map file */
  public static void createMapFile(Path seqFiles) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path merged = new Path(seqFiles.toString() + ".merged");
    mergeSequenceFiles(seqFiles, merged);
    fs.delete(seqFiles, true);
    fs.rename(merged, seqFiles);
    
    //fs.rename(new Path(merged, "part-00000"), seqFiles);
    //fs.delete(merged, true);
    
  }
  
  public static void mergeSequenceFiles(Path input, Path output) throws 
    IOException, InterruptedException, ClassNotFoundException {
    /*
    JobConf conf = new JobConf(CreateMapFileFromSeq.class);
    conf.setJobName("Create Map File");
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(VectorWritable.class);
    conf.setMapperClass(org.apache.hadoop.mapred.lib.IdentityMapper.class);
    conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);
    conf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
    conf.setOutputFormat(MapFileOutputFormat.class);
    conf.setInt("io.map.index.interval", indexInterval);
    conf.setBoolean("mapred.output.compress", true);
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.LzoCodec");
    conf.setNumReduceTasks(1);
    conf.setPartitionerClass(UserIDPartitioner.class);
    FileInputFormat.setInputPaths(conf, input);
    FileOutputFormat.setOutputPath(conf, output);
    JobClient.runJob(conf);
    */
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Create MapFileOutputFormat file.");
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(IdentityReducer.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);
    job.getConfiguration().setInt("io.map.index.interval", indexInterval);
    //job.getConfiguration().setBoolean("mapred.output.compress", true);
    //job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.LzoCodec");
    //job.setPartitionerClass(UserIDPartitioner.class);
    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);
    job.waitForCompletion(true);
  }
  
}
