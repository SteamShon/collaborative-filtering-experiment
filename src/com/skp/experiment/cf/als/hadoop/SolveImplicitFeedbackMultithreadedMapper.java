/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.skp.experiment.cf.als.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;
import com.skp.experiment.common.mapreduce.MultithreadedMapMapper;
import com.skp.experiment.math.als.hadoop.ImplicitFeedbackAlternatingLeastSquaresSolver;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.Random;

/**
 * This is extended class for ALS-WR algorithm in mahout.
 * Why re-implement this? followings are reasons.
 * 1) mahout`s implementation don`t take care of cases when U/M matrix is too large. too large matrix will lead to out of heap space
 * 2) mahout`s implementation use single thread to calculate ui or mj(feature vector).
 * once whole U/M matrix is loaded into memory, we can actually increase speed using multiple thread to calculate ui or mj.
 * 3) mahout`s implementation spend many time to load matrix into memory. multiple thread to load hdfs data into memory can speed up setup process.
 * 
 * 
 */
public class SolveImplicitFeedbackMultithreadedMapper
  extends MultithreadedMapMapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
  private static final Log LOG = LogFactory.getLog(SolveImplicitFeedbackMultithreadedMapper.class);
  public static final String NUM_FEATURES = SolveImplicitFeedbackMultithreadedMapper.class.getName() + ".numFeatures";
  public static final String LAMBDA = SolveImplicitFeedbackMultithreadedMapper.class.getName() + ".lambda";
  public static final String ALPHA = SolveImplicitFeedbackMultithreadedMapper.class.getName() + ".alpha";
  public static final String FEATURE_MATRIX = SolveImplicitFeedbackMultithreadedMapper.class.getName() + ".featureMatrix";
  public static final String NUM_ROWS = SolveImplicitFeedbackMultithreadedMapper.class.getName() + ".numRows";
  public static final String NUM_USERS = SolveImplicitFeedbackMultithreadedMapper.class.getName() + ".numUsers";
  public static final String NUM_ITEMS = SolveImplicitFeedbackMultithreadedMapper.class.getName() + ".numItems";
  public static final String FEATURE_MATRIX_TRANSPOSE = SolveImplicitFeedbackMultithreadedMapper.class.getName() + ".featureMatrixTranspose";
  public static final String LOCK_FILE = SolveImplicitFeedbackMultithreadedMapper.class.getName() + ".lockFile";
  public static final String LOCK_FILE_NUMS = SolveImplicitFeedbackMultithreadedMapper.class.getName() + ".lockFileNums";
  
  private String lockPath = null;
  private long sleepPeriod = 30000;
  private int lockNums;
  private Path currentLockPath = null;
  
  private static Matrix Y;
  private static Matrix YtransposeY;
  private static ImplicitFeedbackAlternatingLeastSquaresSolver solver;
  
  @Override
  protected void setup(Context ctx) throws IOException, InterruptedException {
    /** parse parameters from configuration */
    Configuration conf = ctx.getConfiguration();
    double lambda = Double.parseDouble(ctx.getConfiguration().get(LAMBDA));
    double alpha = Double.parseDouble(ctx.getConfiguration().get(ALPHA));
    int numFeatures = ctx.getConfiguration().getInt(NUM_FEATURES, -1);
    int numRows = ctx.getConfiguration().getInt(NUM_ROWS, -1);
    Path YPath = new Path(ctx.getConfiguration().get(FEATURE_MATRIX));
    Path YtransposeYPath = new Path(ctx.getConfiguration().get(FEATURE_MATRIX_TRANSPOSE));
    
    /** set file lock if necessary */
    lockPath = conf.get(LOCK_FILE); 
    lockNums = conf.getInt(LOCK_FILE_NUMS, 1);
    if (lockPath != null) {
      checkLock(ctx, lockNums);
    }
    /** load necessary matrix U/M into memory */
    Y = ALSMatrixUtil.readMatrixByRowsMultithred(YPath, ctx.getConfiguration(), numRows, numFeatures);
    YtransposeY = ALSMatrixUtil.readDistributedRowMatrix(YtransposeYPath, numFeatures, numFeatures);
    /** initiate linear solver */
    solver = new ImplicitFeedbackAlternatingLeastSquaresSolver(numFeatures, lambda, alpha, Y, YtransposeY);
    LOG.info("Matrix dimension in memory " + Y.rowSize() + "," + Y.columnSize());
    Preconditions.checkArgument(numFeatures > 0, "numFeatures was not set correctly!");
  }
  /** create file lock per each datanode to prevent too many map task simultaneously 
   *  runs on same datanode */ 
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
  protected void cleanup(Context context) throws IOException,
      InterruptedException {
    if (currentLockPath != null) {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      fs.deleteOnExit(currentLockPath);
    }
  }
  @Override
  protected void map(IntWritable userOrItemID, VectorWritable ratingsWritable, Context ctx)
      throws IOException, InterruptedException {
    Vector ratings = new SequentialAccessSparseVector(ratingsWritable.get());

    Vector uiOrmj = solver.solve(ratings);
    ctx.write(userOrItemID, new VectorWritable(uiOrmj));
  }
}
