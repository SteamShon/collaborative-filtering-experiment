package com.skp.experiment.common.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/*
 * CompositeJoinKey
 * 
 * This class hold keys from SRC or TGT table with suffix
 * JOIN A, B, C => A`s suffix is 2, B is 1, C is 0 
 */
public class CompositeJoinKey implements WritableComparable<CompositeJoinKey> {
  private String joinKey = "";
  private int suffix = 0;
  
  public void set(String strJoinKey, int nth) {
    this.joinKey = strJoinKey;
    this.suffix = nth;
  }
  public String getJoinKey() {
    return this.joinKey;
  }
  public int getSuffix() {
    return this.suffix;
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    joinKey = in.readUTF();
    suffix = in.readInt();
  }
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(joinKey);
    out.writeInt(suffix);
  }
  @Override
  public int compareTo(CompositeJoinKey o) {
    if (this.joinKey.compareTo(o.joinKey) != 0) {
      return this.joinKey.compareTo(o.joinKey);
    } else if (this.suffix != o.suffix) {
      return suffix < o.suffix ? -1 : 1;
    } else {
      return 0;
    }
  }
  
  public static class CompositeJoinKeyComparator extends WritableComparator {

    protected CompositeJoinKeyComparator() {
      super(CompositeJoinKey.class);
    }
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1, l1, b2, s2, l2);
    }
  }
  
  static {
    //register
    WritableComparator.define(CompositeJoinKey.class, new CompositeJoinKeyComparator());
  }
}
