package com.skp.experiment.math.matrix.dense;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MatrixValue implements Writable {
  private int index1;
  private int index2;
  private float v;
  
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

  public float getV() {
    return v;
  }

  public void setV(float v) {
    this.v = v;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    index1 = in.readInt();
    index2 = in.readInt();
    v = in.readFloat();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(index1);
    out.writeInt(index2);
    out.writeFloat(v);
  }

}
