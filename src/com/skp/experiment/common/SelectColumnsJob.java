package com.skp.experiment.common;

import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import com.skp.experiment.common.mapreduce.ColumnSelectMapper;

public class SelectColumnsJob extends AbstractJob {
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new SelectColumnsJob(), args);
  }
  @Override
	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();
		addOption("columnIndexs", "cidxs", "column indexs seperated by ,");
		addOption("transformIndex", "tidx", "log transform column index", "-1");
		Map<String, String> parsedArgs = parseArguments(args);
		if (parsedArgs == null) {
			return -1;
		}
		Job selectJob = prepareJob(getInputPath(), getOutputPath(), TextInputFormat.class,
		        ColumnSelectMapper.class, NullWritable.class, Text.class,
		        TextOutputFormat.class);
		selectJob.getConfiguration().set(ColumnSelectMapper.COLUMN_INDEXS, getOption("columnIndexs"));
		selectJob.getConfiguration().setInt(ColumnSelectMapper.TRANSFORM_INDEX, 
		    Integer.parseInt(getOption("transformIndex")));
		selectJob.waitForCompletion(true);
		return 0;
	}

}
