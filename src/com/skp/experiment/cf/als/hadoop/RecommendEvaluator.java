package com.skp.experiment.cf.als.hadoop;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import com.skp.experiment.cf.evaluate.hadoop.EvaluatorJob;
import com.skp.experiment.common.KeyValuesCountJob;
import com.skp.experiment.common.join.ImprovedRepartitionJoinAndFilterJob;
/**
 * <h1>Evaluate recommendations using MAP, precision, recall, mean average percentage metrics </h1>
 * <p>Just wrapper job to run evaluation on recommendation verses probeSet. </p>
 *
 * <p>Command line arguments specific to this class are:</p>
 *
 * <ol>
 * <li>--input (path): recommendations.  </li>
 * <li>--output (path): path where output should go</li>
 * <li>--probeSet (path): path to probeSet. probeSet should be converted into index since recommendations is also converted.</li>
 * <li>--topK (int): topK threshold for recommendations per key.</li>
 * </ol>
 */
public class RecommendEvaluator extends AbstractJob {
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RecommendEvaluator(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption("probeSet", null, "path to test set.");
    addOption("topK", null, "threshold for evaluation", String.valueOf(100));
    
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    Path probeSet = new Path(getOption("probeSet"));
    int topK = getOption("topK") == null || getOption("topK").equals("") ? 100 : Integer.parseInt(getOption("topK"));
    
    /** step 1. get user-item_count */
    KeyValuesCountJob userItemCntJob = new KeyValuesCountJob();
    userItemCntJob.setConf(getConf());
    if (userItemCntJob.run(new String[]{
        "-i", probeSet.toString(), "-o", getTempPath("userItemCnts").toString(), 
        "-inType", "text", 
        "-outType", "text"}) != 0) {
      return -1; 
    }
    // <user_id:string, item_count:integer> 

    /** step 2. inner join with recommendations and probeSet */
    ImprovedRepartitionJoinAndFilterJob joinJob = new ImprovedRepartitionJoinAndFilterJob();
    joinJob.setConf(getConf());
    if (joinJob.run(new String[]{
        "-i", getInputPath().toString(), "-o", getTempPath("recAndTest").toString(),
        "-sidx", "0", "-tgt", getTempPath("userItemCnts").toString() + ":0:0:1:inner"}) != 0) {
      return -1;
    }
    // <user_id, item_id, score, reason_id, date, item_count> 
    
    /** step 3. left outer join with rec and recAndTest */
    ImprovedRepartitionJoinAndFilterJob outerJob = new ImprovedRepartitionJoinAndFilterJob();
    outerJob.setConf(getConf());
    if (outerJob.run(new String[]{
        "-i", getTempPath("recAndTest").toString(), "-o", getTempPath("recAndTestMerged").toString(),
        "-sidx", "0,1", "-tgt", 
        probeSet.toString() + ":0,1:0,1:3:outer", "--defaultValue", "-1.0"}) != 0) {
      return -1;
    }
    // <user_id, item_id, score, reason_id, date, item_count, flag(0 or 1)>
    EvaluatorJob evalJob = new EvaluatorJob();
    evalJob.setConf(getConf());
    if (evalJob.run(new String[]{
        "-i", getTempPath("recAndTestMerged").toString(), "-o", getTempPath("getOutput").toString(),
        "--topK", String.valueOf(topK)}) != 0) {
      return -1;
    }
    return 0;
  }
}
