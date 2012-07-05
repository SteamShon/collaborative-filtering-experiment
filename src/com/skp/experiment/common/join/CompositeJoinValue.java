package com.skp.experiment.common.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CompositeJoinValue implements Writable{
  private String value = "";
  private int suffix = 0;
  public void set(String strValue, int nth) {
    this.value = strValue;
    this.suffix = nth;
  }
  public String getValue() {
    return this.value;
  }
  public int getSuffix() {
    return this.suffix;
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readUTF();
    suffix = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(value);
    out.writeInt(suffix);
  }
}
