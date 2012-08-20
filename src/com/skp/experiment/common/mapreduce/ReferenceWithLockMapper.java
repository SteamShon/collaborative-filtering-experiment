package com.skp.experiment.common.mapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class ReferenceWithLockMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT, K extends WritableComparable, V extends Writable> 
  extends ReferenceMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT, K, V> {
  public static final String LOCK_PATH = ReferenceWithLockMapper.class.getName() + ".lockPath";
  public static final String LOCK_NUMS = ReferenceWithLockMapper.class.getName() + ".lockNums";
  
  private String lockPath = null;
  private long sleepPeriod = 30000;
  private int lockNums;
  private Path currentLockPath = null;
  
  @Override
  protected void setup(Context ctx)
      throws IOException, InterruptedException {
    super.setup(ctx);
    Configuration conf = ctx.getConfiguration();
    lockPath = conf.get(LOCK_PATH);
    lockNums = conf.getInt(LOCK_NUMS, 1);
    if (lockPath != null) {
      checkLock(ctx, lockNums);
    }
  }
  private void checkLock(Context ctx, int lockNums) throws InterruptedException, IOException {
    InetAddress thisIp =InetAddress.getLocalHost();
    String hostIp = thisIp.getHostAddress();
    
    // busy wait
    Configuration conf = ctx.getConfiguration();
    long totalSleep = 0;
    boolean haveLock = false;
    FileSystem fs = FileSystem.get(conf);
    while (haveLock == false) {
      for (int i = 0; i < lockNums; i++) {
        Path checkPath = new Path(lockPath, hostIp + "_" + i);
        if (fs.exists(checkPath) == false) {
          haveLock = true;
          currentLockPath = checkPath;
          BufferedWriter br = new BufferedWriter(
              new OutputStreamWriter(fs.create(currentLockPath)));
          br.write(ctx.getTaskAttemptID().toString());
          break;
        }
      }
      if (haveLock == false) {
        Random random = new Random();
        int diff = 1000 + random.nextInt(1000) % 1000;
        totalSleep += diff + sleepPeriod;
        ctx.setStatus("sleeping: " + String.valueOf(totalSleep));
        Thread.sleep(sleepPeriod + diff);
      } 
    }
  } 
  @Override
  protected void cleanup(Context ctx) throws IOException,
      InterruptedException {
    super.cleanup(ctx);
    if (currentLockPath != null) {
      FileSystem fs = FileSystem.get(ctx.getConfiguration());
      fs.deleteOnExit(currentLockPath);
    }
  }
  
}
