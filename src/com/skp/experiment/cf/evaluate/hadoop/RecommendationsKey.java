package com.skp.experiment.cf.evaluate.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/*
 * define user, item key
 */
public class RecommendationsKey implements WritableComparable<RecommendationsKey> {
  protected Double epsilon = 10e-7;
  //protected Integer userID = 0;
  protected String userID;
  protected Double rate = 0.0;
  protected Integer itemCount = 0;
  /*
  public void set(int userID, double rate) {
    this.userID = userID;
    this.rate = rate;
  }
  public void set(int userID, double rate, int itemCount) {
    this.userID = userID;
    this.rate = rate;
    this.itemCount = itemCount;
  }
  
  public int getUserID() {
    return userID;
  }
  */
  public void set(String userID, double rate) {
    this.userID = userID;
    this.rate = rate;
  }
  public void set(String userID, double rate, int itemCount) {
    this.userID = userID;
    this.rate = rate;
    this.itemCount = itemCount;
  }
  public String getUserID() {
    return userID;
  }
  public double getRate() {
    return rate;
  }
  public int getItemCount() {
    return itemCount;
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    //userID = in.readInt();
    userID = in.readUTF();
    rate = in.readDouble();
    itemCount = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    //out.writeInt(userID);
    out.writeUTF(userID);
    out.writeDouble(rate);
    out.writeInt(itemCount);
  }
  
  
  @Override
  public int hashCode() {
    return userID.hashCode();
  }
  /*
  @Override
  public int compareTo(RecommendationsKey o) {
    if (this.userID != o.userID) {
      return this.userID < o.userID ? -1 : 1;
    } else if (Math.abs(this.rate - o.rate) >= epsilon) {
      return this.rate < o.rate ? -1 : 1;
    } else {
      return 0;
    }
  }
  */
  @Override
  public int compareTo(RecommendationsKey o) {
    if (this.userID.compareTo(o.userID) != 0) {
      return this.userID.compareTo(o.userID);
    } else if (Math.abs(this.rate - o.rate) >= epsilon) {
      return this.rate < o.rate ? -1 : 1;
    } else {
      return 0;
    }
  }
  public static class RecommendationsKeyComparator extends WritableComparator {
    public RecommendationsKeyComparator() {
      super(RecommendationsKey.class);
    }
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1, l1, b2, s2, l2);
    }
  }
  
  static {
    // register
    WritableComparator.define(RecommendationsKey.class, new RecommendationsKeyComparator());
  }
}
