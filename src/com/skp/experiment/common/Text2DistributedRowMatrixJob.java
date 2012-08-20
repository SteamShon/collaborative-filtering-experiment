package com.skp.experiment.common;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Text2DistributedRowMatrixJob extends AbstractJob {
  private static final Logger log = LoggerFactory.getLogger(Text2DistributedRowMatrixJob.class);
	public static final String ROW_IDX_KEY = "rowidx";
	public static final String COL_IDX_KEY = "colidx";
	public static final String VALUE_IDX_KEY = "valueidx";
	public static final String NUM_COLS_KEY = "numCols";
	public static final String SEQUENTIAL = "sequential";
	public static final String OUT_KEY_TYPE = "outKeyType";
	private static String DELIMETER = ",";
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Text2DistributedRowMatrixJob(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();
		
		addOption(ROW_IDX_KEY, "ri", "row index");
		addOption(COL_IDX_KEY, "ci", "column index");
		addOption(VALUE_IDX_KEY, "vi", "value index", String.valueOf(-1));
		addOption(NUM_COLS_KEY, "nc", "number of columns", String.valueOf(Integer.MAX_VALUE));
		addOption("delimeter", null, "delimeter", ",");
		addOption(SEQUENTIAL, null, "sequential vector", false);
		addOption(OUT_KEY_TYPE, null, "output key type class", "IntWritable");
		
		Map<String, String> parsedArg = parseArguments(args);
		if (parsedArg == null) {
			return -1;
		}
		Class<? extends WritableComparable> outputKeyClass = 
		    getOption(OUT_KEY_TYPE).toLowerCase().equals("text") ? Text.class : IntWritable.class;
		
		Job job = prepareJob(getInputPath(), getOutputPath(), TextInputFormat.class, 
				Text2DistributedRowMatrixMapper.class, outputKeyClass, VectorWritable.class, 
				Text2DistributedRowMatrixReducer.class, outputKeyClass, VectorWritable.class,
				SequenceFileOutputFormat.class);
		
		Configuration conf = job.getConfiguration();
		conf.setInt(ROW_IDX_KEY, Integer.parseInt(getOption(ROW_IDX_KEY)));
		conf.setInt(COL_IDX_KEY, Integer.parseInt(getOption(COL_IDX_KEY)));
		conf.setInt(VALUE_IDX_KEY, Integer.parseInt(getOption(VALUE_IDX_KEY)));
    conf.setInt(NUM_COLS_KEY, Integer.parseInt(getOption(NUM_COLS_KEY)));
		conf.setBoolean(SEQUENTIAL, getOption(SEQUENTIAL) == null ? false : true);
		conf.set(OUT_KEY_TYPE, getOption(OUT_KEY_TYPE).toLowerCase());
		
		job.setJarByClass(Text2DistributedRowMatrixJob.class);
		job.setCombinerClass(Text2DistributedRowMatrixReducer.class);
		job.waitForCompletion(true);
		return 0;
	}
	
	public static class Text2DistributedRowMatrixMapper extends 
		Mapper<LongWritable, Text, WritableComparable, VectorWritable> {
		private static Integer rowIdx;
		private static Integer colIdx;
		private static Integer valueIdx;
		private static Integer numCols;
		private static Boolean sequential;
		private static IntWritable outRowID = new IntWritable();
		private static Text outRowIDText = new Text();
		private static Boolean textOutKey = false;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			rowIdx = conf.getInt(ROW_IDX_KEY, 0);
			colIdx = conf.getInt(COL_IDX_KEY, 1);
			valueIdx = conf.getInt(VALUE_IDX_KEY, 2);
      numCols = conf.getInt(NUM_COLS_KEY, Integer.MAX_VALUE);

      sequential = Boolean.parseBoolean(conf.get(SEQUENTIAL));
			if (conf.get(OUT_KEY_TYPE).equals("text")) {
			  textOutKey = true;
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(DELIMETER);
			try {
  			if (textOutKey) {
  			  outRowIDText.set(tokens[rowIdx]);
  			} else {
  			  outRowID.set(Integer.parseInt(tokens[rowIdx]));
  			}
  			int index = Integer.parseInt(tokens[colIdx]);
  			float pref = 1;
  			if (valueIdx >= 0) {
  			  pref = Float.parseFloat(tokens[valueIdx]);
  			}
  			Vector outVector = null;
  			if (sequential) {
  			  outVector = new SequentialAccessSparseVector(numCols, 1);
  			} else {
  			  outVector = new RandomAccessSparseVector(numCols, 1);
  			}
  			outVector.set(index, pref);
  			if (textOutKey) {
  			  context.write(outRowIDText, new VectorWritable(outVector));
  			} else {
  			  context.write(outRowID, new VectorWritable(outVector));
  			}
			} catch (Exception e) {
			  log.info(Text2DistributedRowMatrixJob.class.getName() + ":" + value.toString());
			}
		}
	}
	public static class Text2DistributedRowMatrixReducer extends 
		Reducer<WritableComparable, VectorWritable, WritableComparable, VectorWritable> {
		@Override
		protected void reduce(WritableComparable rowid, Iterable<VectorWritable> partialVectors,
				Context ctx)
				throws IOException, InterruptedException {
			Vector merged = Vectors.merge(partialVectors);
			ctx.write(rowid, new VectorWritable(merged));
		}

	}
}
