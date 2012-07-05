package com.skp.experiment.cf.creditcf.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

class ItemRatingVectorsMapper extends Mapper<LongWritable,Text,IntWritable,VectorWritable> {
  @Override
  protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
    String[] tokens = TasteHadoopUtils.splitPrefTokens(line.toString());
    int userID = Integer.parseInt(tokens[0]);
    int itemID = Integer.parseInt(tokens[1]);
    float rating = Float.parseFloat(tokens[2]);

    Vector ratings = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
    ratings.set(userID, rating);

    ctx.write(new IntWritable(itemID), new VectorWritable(ratings, true));
  }
}