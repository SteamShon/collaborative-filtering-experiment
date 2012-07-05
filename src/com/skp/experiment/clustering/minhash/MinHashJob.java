package com.skp.experiment.clustering.minhash;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.minhash.MinHashReducer;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.commandline.MinhashOptionCreator;
import org.apache.mahout.math.VectorWritable;

import com.skp.experiment.common.Text2DistributedRowMatrixJob;

public class MinHashJob extends AbstractJob {
  private static final String VECTORIZED_INPUT_PATH = MinHashJob.class.getName() + ".vectorized";
  private int minClusterSize;
  private int minVectorSize;
  private String hashType;
  private int numHashFunctions;
  private int keyGroups;
  private int numReduceTasks;
  private boolean debugOutput;
  private boolean isVectorizedInput;
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run( new MinHashJob(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(MinhashOptionCreator.MIN_CLUSTER_SIZE, 
        "minClusterSize", "minimum number of users belong to cluster.", 
        String.valueOf(0));
    addOption(MinhashOptionCreator.MIN_VECTOR_SIZE, 
        "minVectorSize", "minimum number of item each user purchased.",
        String.valueOf(0));
    addOption(MinhashOptionCreator.HASH_TYPE, "hashType", 
        "hash type: (linear, polynomial, murmur)", "murmur");
    addOption(MinhashOptionCreator.NUM_HASH_FUNCTIONS, "numHashFunctions",
        "number of hash functions.", String.valueOf(20));
    addOption(MinhashOptionCreator.KEY_GROUPS, "keyGroups", 
        "number of key groups.", String.valueOf(4));
    addOption(MinhashOptionCreator.NUM_REDUCERS, "numReduceTasks",
        "number of reducer tasks.", String.valueOf(10));
    addOption(MinhashOptionCreator.DEBUG_OUTPUT, "debugOutput", 
        "true if want to print out debug.", false);
    addOption(DefaultOptionCreator.OVERWRITE_OPTION, "overwrite", 
        "true if want to overwrite.", false);
    addOption("isVectorizedInput", "vinput", "true if input is vectorized before. default is csv format input", false);
    if (parseArguments(args) == null) {
      return -1;
    }
    
    isVectorizedInput = Boolean.parseBoolean(getOption("isVectorizedInput"));
    Path input;
    if (!isVectorizedInput) {
      Path tmpOut = getTempPath(VECTORIZED_INPUT_PATH);
      ToolRunner.run(new Text2DistributedRowMatrixJob(), new String[]{
        "-i", getInputPath().toString(), "-o", tmpOut.toString(),
        "-ri", "0", "-ci", "1", "-vi", "1", "--outKeyType", "text"
      });
      input = tmpOut;
    } else {
      input = getInputPath();
    }
    Path output = getOutputPath();
    if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
      HadoopUtil.delete(getConf(), output);
    }
    minClusterSize = Integer.valueOf(getOption(MinhashOptionCreator.MIN_CLUSTER_SIZE));
    minVectorSize = Integer.valueOf(getOption(MinhashOptionCreator.MIN_VECTOR_SIZE));
    hashType = getOption(MinhashOptionCreator.HASH_TYPE);
    numHashFunctions = Integer.valueOf(getOption(MinhashOptionCreator.NUM_HASH_FUNCTIONS));
    keyGroups = Integer.valueOf(getOption(MinhashOptionCreator.KEY_GROUPS));
    numReduceTasks = Integer.parseInt(getOption(MinhashOptionCreator.NUM_REDUCERS));
    debugOutput = Boolean.parseBoolean(MinhashOptionCreator.DEBUG_OUTPUT);
    
    Configuration conf = getConf();
    conf.setInt(MinhashOptionCreator.MIN_CLUSTER_SIZE, minClusterSize);
    conf.setInt(MinhashOptionCreator.MIN_VECTOR_SIZE, minVectorSize);
    conf.set(MinhashOptionCreator.HASH_TYPE, hashType);
    conf.setInt(MinhashOptionCreator.NUM_HASH_FUNCTIONS, numHashFunctions);
    conf.setInt(MinhashOptionCreator.KEY_GROUPS, keyGroups);
    conf.setBoolean(MinhashOptionCreator.DEBUG_OUTPUT, debugOutput);

    Class<? extends Writable> outputClass = debugOutput ? VectorWritable.class : Text.class;
    Class<? extends OutputFormat> outputFormatClass =
        debugOutput ? SequenceFileOutputFormat.class : TextOutputFormat.class;
    Job job = prepareJob(input, output, SequenceFileInputFormat.class, 
        MinHashMapper.class, Text.class, outputClass, 
        MinHashReducer.class, Text.class, outputClass, outputFormatClass
        );
    job.setNumReduceTasks(numReduceTasks);
    job.waitForCompletion(true);
    
    //FileSystem fs = FileSystem.get(getConf());
    //fs.delete(getTempPath(VECTORIZED_INPUT_PATH), true);
    
    return 0;
  }

}
