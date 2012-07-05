package com.skp.experiment.math.matrix.dense;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class MatrixKeyPartitioner extends Partitioner<MatrixKey, MatrixValue> implements Configurable {
  public static final String J_BLOCK_SIZE = MatrixKeyPartitioner.class.getName() + ".JBlockSize";
  private Configuration conf;
  private static int JB;
  @Override
  public int getPartition(MatrixKey key, MatrixValue value, int numPartitions) {
    JB = Integer.parseInt(conf.get(J_BLOCK_SIZE));
    return (key.getIndex1() * JB + key.getIndex2()) % numPartitions;
  }

  @Override
  public Configuration getConf() {
   return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}
