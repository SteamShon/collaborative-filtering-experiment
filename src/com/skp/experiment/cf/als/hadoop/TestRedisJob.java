package com.skp.experiment.cf.als.hadoop;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import com.google.common.primitives.Ints;

public class TestRedisJob extends AbstractJob {
  private static final String HOST_NAME = "20.20.20.31";
  private static final Logger log = LoggerFactory.getLogger(TestRedisJob.class);
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new TestRedisJob(), args);
  }
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("userRatings", null, "user ratings");
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    try {
      Job job = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class, 
      Map1.class, IntWritable.class, VectorWritable.class, SequenceFileOutputFormat.class);
      job.waitForCompletion(true);
      
      FileSystem fs = FileSystem.get(getConf());
      fs.delete(getOutputPath(), true);
      
      Job job2 = prepareJob(new Path(getOption("userRatings")), getOutputPath(), SequenceFileInputFormat.class, 
          Map2.class, IntWritable.class, VectorWritable.class, SequenceFileOutputFormat.class);
      job2.waitForCompletion(true);
    } finally {
      if (!flushAll()) {
        return -1;
      }
    }
    
    return 0;
  }
  
  private static boolean flushAll() {
    log.info("check redis server from local");
    JedisPool pool = null;
    Jedis jedis = null;
    Pipeline pipeline = null;
    try {
      pool = new JedisPool(new JedisPoolConfig(), HOST_NAME);
      jedis = pool.getResource();
      pipeline = jedis.pipelined();
      jedis.flushAll();
    } catch (Exception e) {
      return false;
    } finally {
      pipeline.exec();
      pool.returnResource(jedis);
      pool.destroy();
    }
    return true;
  }
  private static String generateRedisProto(String... args) {
    StringBuffer sb = new StringBuffer();
    sb.append("*" + args.length + "\r\n");
    for (String arg : args) {
      sb.append("$" + arg.getBytes().length + "\r\n");
      sb.append(arg + "\r\n");
    }
    return sb.toString();
  }
  public static byte[] toByteArray(double value) {
    byte[] bytes = new byte[8];
    ByteBuffer.wrap(bytes).putDouble(value);
    return bytes;
  }
  private static class Map1 extends Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
    private static JedisPool pool;
    private static Jedis jedis;
    private static Pipeline pipeline;
    private static long sum = 0;
    private static long cnt = 0;
    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
        throws IOException, InterruptedException {
      Vector v = value.get();
      Iterator<Vector.Element> iter = v.iterateNonZero();
      long start = System.currentTimeMillis();
      pipeline.set(Ints.toByteArray(key.get()), buildOutput(v).getBytes());
      sum += System.currentTimeMillis() - start;
      cnt++;
      /*
      while(iter.hasNext()) {
        Vector.Element e = iter.next();
        pipeline.rpush(Ints.toByteArray(key.get()), toByteArray(e.get()));
      } 
      */
      
    }
    private static String buildOutput(Vector v) {
      StringBuffer sb = new StringBuffer();
      Iterator<Vector.Element> iter = v.iterateNonZero();
      int idx = 0;
      while (iter.hasNext()) {
        Vector.Element e = iter.next();
        if (idx++ != 0) {
          sb.append(",");
        }
        sb.append(e.get());
      }
      return sb.toString();
    }
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      JedisPoolConfig jedisConf = new JedisPoolConfig();
      jedisConf.setMaxWait(120000);
      pool = new JedisPool(jedisConf, HOST_NAME);
      
      jedis = pool.getResource();
      pipeline = jedis.pipelined();
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      log.info("Average access time: " + (sum / (double)cnt));
      pipeline.exec();
      pool.returnResource(jedis);
      pool.destroy();
    }
  }
  private static class Map2 extends Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
    private static JedisPool pool;
    private static Jedis jedis;
    private static long sum = 0;
    private static long cnt = 0;
    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      pool.returnResource(jedis);
      pool.destroy();
    }
    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
        throws IOException, InterruptedException {
      Vector userRatings = value.get();
      Iterator<Vector.Element> ratings = userRatings.iterateNonZero();
      while (ratings.hasNext()) {
        Vector.Element r = ratings.next();
        long start = System.currentTimeMillis();
        String features = jedis.get(r.toString());
        sum += System.currentTimeMillis() - start;
        cnt++;
      }
    }
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      JedisPoolConfig jedisConf = new JedisPoolConfig();
      jedisConf.setMaxWait(120000);
      pool = new JedisPool(jedisConf, HOST_NAME);
      jedis = pool.getResource();
      context.setStatus("Average access time: " + ((double)sum / cnt));
    }
    
  }
}
