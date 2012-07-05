package com.skp.experiment.common.join;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;


public class JoinOptionUtils {
  public static final String SRC_TABLE_OPTIONS = JoinOptionUtils.class.getName() + ".sourceTableOptions";
  public static final String TGT_TABLE_OPTIONS = JoinOptionUtils.class.getName() + ".targetTableOptions";
  
  public static final String INTER_DELIMETER = "::";
  public static final String DELIMETER = ",";
  public static final String NULL_STR = "";
  
  public static List<JoinOption> parseOptionStrings(String optionStrs) {
    List<JoinOption> result = new ArrayList<JoinOption>();
    String[] options = optionStrs.split(INTER_DELIMETER);
    for (String optionStr : options) {
      JoinOption option = new JoinOption();
      option.parseOption(optionStr);
      result.add(option);
    }
    return result;
  }
  public static JoinOption parseOptionString(String optionStr) {
    JoinOption option = new JoinOption();
    option.parseOption(optionStr);
    return option;
  }
  public static String[] splitPrefTokens(String line) {
    return line.split(DELIMETER);
  }
  public static String fetchFileds(String[] tokens, List<Integer> keys) {
    
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < keys.size(); i++) {
      if (i != 0) {
        sb.append(DELIMETER);
      }
      //System.out.println("token(" + keys.get(i) + "):\t" + tokens[keys.get(i)]);
      if (keys.get(i) >= 0 && keys.get(i) < tokens.length) {
        sb.append(tokens[keys.get(i)]);
      } else {
        sb.append(NULL_STR);
      }
    }
    return sb.toString();
  }
}
