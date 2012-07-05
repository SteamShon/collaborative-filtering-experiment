package com.skp.experiment.math.matrix.dense;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MatrixKey implements WritableComparable<MatrixKey> {
  private int index1;
  private int index2;
  private int index3;
  public byte m;
  
  public int getIndex1() {
    return index1;
  }

  public void setIndex1(int index1) {
    this.index1 = index1;
  }

  public int getIndex2() {
    return index2;
  }

  public void setIndex2(int index2) {
    this.index2 = index2;
  }

  public int getIndex3() {
    return index3;
  }

  public void setIndex3(int index3) {
    this.index3 = index3;
  }

  public byte getM() {
    return m;
  }

  public void setM(byte m) {
    this.m = m;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    index1 = in.readInt();
    index2 = in.readInt();
    index3 = in.readInt();
    m = in.readByte();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(index1);
    out.writeInt(index2);
    out.writeInt(index3);
    out.writeByte(m);
  }

  @Override
  public int compareTo(MatrixKey other) {
    MatrixKey o = (MatrixKey) other;
    if (this.index1 < o.index1) {
      return -1;
    } else if (this.index1 > o.index1) {
      return +1;
    }
    if (this.index2 < o.index2) {
      return -1;
    } else if (this.index2 > o.index2) {
      return +1;
    }
    if (this.index3 < o.index3) {
      return -1;
    } else if (this.index3 > o.index3) {
      return +1;
    }
    if (this.m < o.m) {
      return -1;
    } else if (this.m > o.m) {
      return +1;
    }
    return 0;
  }

}
