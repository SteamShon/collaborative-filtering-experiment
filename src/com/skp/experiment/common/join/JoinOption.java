package com.skp.experiment.common.join;

import java.util.List;

import com.skp.experiment.common.OptionParseUtil;


public class JoinOption {
  public static final String DELIMETER = ":";
  public static final String INNER_DELIMETER = ",";
  
  private String table;
  private List<Integer> sourceTableKeyIndexs;
  private List<Integer> targetTableKeyIndexs;
  private List<Integer> targetTableValueIndexs;
  private String type;
  private int optionColumnSize = 5;
  
  public String getTable() {
    return table;
  }
  public void setTable(String table) {
    this.table = table;
  }
  
  public List<Integer> getSourceTableKeyIndexs() {
    return sourceTableKeyIndexs;
  }
  public void setSourceTableKeyIndexs(List<Integer> sourceTableKeyIndexs) {
    this.sourceTableKeyIndexs = sourceTableKeyIndexs;
  }
  
  public List<Integer> getTargetTableKeyIndexs() {
    return targetTableKeyIndexs;
  }
  public void setTargetTableKeyIndexs(List<Integer> targetTableKeyIndexs) {
    this.targetTableKeyIndexs = targetTableKeyIndexs;
  }
  public List<Integer> getTargetTableValueIndexs() {
    return targetTableValueIndexs;
  }
  public void setTargetTableValueIndexs(List<Integer> targetTableValueIndexs) {
    this.targetTableValueIndexs = targetTableValueIndexs;
  }
  public String getType() {
    return type;
  }
  public void setType(String type) {
    this.type = type;
  }
  public boolean parseOption(String optionStr) {
    String[] tokens = optionStr.split(DELIMETER);
    if (tokens.length != optionColumnSize) return false;
    this.table = tokens[0];
    this.sourceTableKeyIndexs = OptionParseUtil.decode(tokens[1], INNER_DELIMETER);
    this.targetTableKeyIndexs = OptionParseUtil.decode(tokens[2], INNER_DELIMETER);
    this.targetTableValueIndexs = OptionParseUtil.decode(tokens[3], INNER_DELIMETER);
    this.type = tokens[4];
    return true;
  }
  private String buildInnerElement(List<Integer> indexs) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < indexs.size(); i++) {
      sb.append(indexs.get(i));
      if (i < indexs.size() - 1) {
        sb.append(INNER_DELIMETER);
      }
    }
    return sb.toString();
  }
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(table).append(DELIMETER);
    sb.append(buildInnerElement(sourceTableKeyIndexs)).append(DELIMETER);
    sb.append(buildInnerElement(targetTableKeyIndexs)).append(DELIMETER);
    sb.append(buildInnerElement(targetTableValueIndexs)).append(DELIMETER);
    sb.append(this.type);
    return sb.toString();
  }
}