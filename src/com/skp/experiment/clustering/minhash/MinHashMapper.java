package com.skp.experiment.clustering.minhash;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.minhash.HashFactory;
import org.apache.mahout.clustering.minhash.HashFactory.HashType;
import org.apache.mahout.clustering.minhash.HashFunction;
import org.apache.mahout.common.commandline.MinhashOptionCreator;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinHashMapper extends Mapper<Text, VectorWritable, Text, Writable>{
  private static final Logger log = LoggerFactory.getLogger(MinHashMapper.class);

  private HashFunction[] hashFunction;
  private int numHashFunctions;
  private int keyGroups;
  private int minVectorSize;
  private boolean debugOutput;
  private int[] minHashValues;
  private byte[] bytesToHash;
  private static final String KEY_VALUE_DELIEMETER = ",";
  private static final String VALUE_DELIMETER = "\t";

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    this.numHashFunctions = conf.getInt(MinhashOptionCreator.NUM_HASH_FUNCTIONS, 10);
    this.minHashValues = new int[numHashFunctions];
    this.bytesToHash = new byte[4];
    this.keyGroups = conf.getInt(MinhashOptionCreator.KEY_GROUPS, 1);
    this.minVectorSize = conf.getInt(MinhashOptionCreator.MIN_VECTOR_SIZE, 5);
    String htype = conf.get(MinhashOptionCreator.HASH_TYPE, "linear");
    this.debugOutput = conf.getBoolean(MinhashOptionCreator.DEBUG_OUTPUT, false);

    HashType hashType;
    try {
      hashType = HashType.valueOf(htype);
    } catch (IllegalArgumentException iae) {
      log.warn("No valid hash type found in configuration for {}, assuming type: {}", htype, HashType.LINEAR);
      hashType = HashType.LINEAR;
    }
    hashFunction = HashFactory.createHashFunctions(hashType, numHashFunctions);
  }
  
  private String buildOutputString(Text item, Vector featureVector) {
    StringBuffer sb = new StringBuffer();
    sb.append(item.toString());
    if (featureVector.getNumNondefaultElements() > 0) {
      sb.append(KEY_VALUE_DELIEMETER);
    }
    int cnt = 0;
    Iterator<Vector.Element> iter = featureVector.iterateNonZero();
    while (iter.hasNext()) {
      Vector.Element ele = iter.next();
      if (cnt++ != 0) {
        sb.append(VALUE_DELIMETER);
      }
      sb.append(ele.index());
    }
    return sb.toString();
  }
  
  @Override
  public void map(Text item, VectorWritable features, Context context) throws IOException, InterruptedException {
    Vector featureVector = features.get();
    if (featureVector.getNumNondefaultElements() < minVectorSize) {
      return;
    }     
    // Initialize the minhash values to highest
    for (int i = 0; i < numHashFunctions; i++) {
      minHashValues[i] = Integer.MAX_VALUE;
    }

    for (int i = 0; i < numHashFunctions; i++) {
      Iterator<Vector.Element> iter = featureVector.iterateNonZero();
      while (iter.hasNext()) {
        Vector.Element ele = iter.next();
        int value = ele.index();
        bytesToHash[0] = (byte) (value >> 24);
        bytesToHash[1] = (byte) (value >> 16);
        bytesToHash[2] = (byte) (value >> 8);
        bytesToHash[3] = (byte) value;
        int hashIndex = hashFunction[i].hash(bytesToHash);
        //if our new hash value is less than the old one, replace the old one
        if (minHashValues[i] > hashIndex) {
          minHashValues[i] = hashIndex;
        }
      }
    }
    
    // output the cluster information
    for (int i = 0; i < numHashFunctions; i++) {
      StringBuilder clusterIdBuilder = new StringBuilder();
      for (int j = 0; j < keyGroups; j++) {
        clusterIdBuilder.append(minHashValues[(i + j) % numHashFunctions]).append('-');
      }
      //remove the last dash
      clusterIdBuilder.deleteCharAt(clusterIdBuilder.length() - 1);
      Text cluster = new Text(clusterIdBuilder.toString());
      Writable point;
      if (debugOutput) {
        point = new VectorWritable(featureVector.clone());
      } else {
        point = new Text(buildOutputString(item, featureVector));
      }
      System.out.println(item.toString() + ":" + buildOutputString(item, featureVector));
      context.write(cluster, point);
    }
  }
}
