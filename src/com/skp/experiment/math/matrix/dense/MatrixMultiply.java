/**	A MapReduce algorithm for matrix multiplication.
 *
 *	<p>For detailed documentation see <a href="http://homepage.mac.com/j.norstad/matrix-multiply">
 *	A MapReduce algorithm for matrix multiplication</a>.
 *
 *	<p>This implementation is for integer matrix elements. Implementations for other element
 *	types would be similar. A general implementation which would work with any element type
 *	that was writeable, addable, and multiplyable would also be possible, but probably not
 *	very efficient.
 */
package com.skp.experiment.math.matrix.dense;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class MatrixMultiply extends AbstractJob {

	/**	True to print debugging messages. */

	private static final boolean DEBUG = false;
	
	/**	Global variables for the mapper and reducer tasks. */
	
	private static String inputPathA;
	private static String inputPathB;
	private static String outputDirPath;
	private static String tempDirPath;
	private static int strategy;
	private static int R1;
	private static int R2;
	private static int I;
	private static int K;
	private static int J;
	private static int IB;
	private static int KB;
	private static int JB;
	
	private static int NIB;
	private static int NKB;
	private static int NJB;
	
	private static boolean useM;
	
	private static int lastIBlockNum;
	private static int lastIBlockSize;
	private static int lastKBlockNum;
	private static int lastKBlockSize;
	private static int lastJBlockNum;
	private static int lastJBlockSize;
	private static float threshold = 0;
	private static double PI = 10e-5;
	/**	The key class for the input and output sequence files and the job 2 intermediate keys. */

	public static class IndexPair implements WritableComparable {
		public int index1;
		public int index2;
		public void write (DataOutput out)
			throws IOException
		{
			out.writeInt(index1);
			out.writeInt(index2);
		}
		public void readFields (DataInput in)
			throws IOException
		{
			index1 = in.readInt();
			index2 = in.readInt();
		}
		public int compareTo (Object other) {
			IndexPair o = (IndexPair)other;
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
			return 0;
		}
		public int hashCode () {
			return index1 << 16 + index2;
		}
	}
	
	/**	The job 1 intermediate key class. */
	
	private static class Key implements WritableComparable {
		public int index1;
		public int index2;
		public int index3;
		public byte m;
		public void write (DataOutput out)
			throws IOException
		{
			out.writeInt(index1);
			out.writeInt(index2);
			out.writeInt(index3);
			if (useM) out.writeByte(m);
		}
		public void readFields (DataInput in)
			throws IOException
		{
			index1 = in.readInt();
			index2 = in.readInt();
			index3 = in.readInt();
			if (useM) m = in.readByte();
		}
		public int compareTo (Object other) {
			Key o = (Key)other;
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
			if (!useM) return 0;
			if (this.m < o.m) {
				return -1;
			} else if (this.m > o.m) {
				return +1;
			}
			return 0;
		}
	}
	
	/**	The job 1 intermediate value class. */
	
	private static class Value implements Writable {
		public int index1;
		public int index2;
		//public int v;
		public float v;
		public void write (DataOutput out)
			throws IOException
		{
			out.writeInt(index1);
			out.writeInt(index2);
			//out.writeInt(v);
			out.writeFloat(v);
		}
		public void readFields (DataInput in)
			throws IOException
		{
			index1 = in.readInt();
			index2 = in.readInt();
			//v = in.readInt();
			v = in.readFloat();
		}
	}
	
	/**	The job 1 mapper class. */
	
	private static class Job1Mapper 
		extends Mapper<LongWritable, Text, Key, Value>
	{
		private Path path;
		private boolean matrixA;
		private Key key = new Key();
		private Value value = new Value();
		
		public void setup (Context context) {
			init(context);
			FileSplit split = (FileSplit)context.getInputSplit();
			path = split.getPath();
			matrixA = path.toString().startsWith(inputPathA);
			if (DEBUG) {
				System.out.println("##### Map setup: matrixA = " + matrixA + " for " + path);
				System.out.println("   strategy = " + strategy);
				System.out.println("   R1 = " + R1);
				System.out.println("   I = " + I);
				System.out.println("   K = " + K);
				System.out.println("   J = " + J);
				System.out.println("   IB = " + IB);
				System.out.println("   KB = " + KB);
				System.out.println("   JB = " + JB);
			}
		}
		
		private void printMapInput (IndexPair indexPair, IntWritable el) {
			System.out.println("##### Map input: (" + indexPair.index1 + "," + 
				indexPair.index2 + ") " + el.get());
		}
		
		private void printMapOutput (Key key, Value value) {
			System.out.println("##### Map output: (" + key.index1 + "," + 
				key.index2 + "," + key.index3 + "," + key.m + ") (" + 
				value.index1 + "," + value.index2 + "," + value.v + ") ");
		}
		
		private void badIndex (int index, int dim, String msg) {
			System.err.println("Invalid " + msg + " in " + path + ": " + index + " " + dim);
			System.exit(1);
		}
		
		public void map (LongWritable offset, Text line, Context context)
			throws IOException, InterruptedException 
		{
		  String[] tokens = line.toString().split(",");
		  IndexPair indexPair = new IndexPair();
		  indexPair.index1 = Integer.parseInt(tokens[0]);
		  indexPair.index2 = Integer.parseInt(tokens[1]);
		  FloatWritable el = new FloatWritable(Float.parseFloat(tokens[2]));
		  //IntWritable el = new IntWritable(Integer.parseInt(tokens[2]));
			//if (DEBUG) printMapInput(indexPair, el);
			int i = 0;
			int k = 0;
			int j = 0;
			if (matrixA) {
				i = indexPair.index1;
				if (i < 0 || i >= I) badIndex(i, I, "A row index");
				k = indexPair.index2;
				if (k < 0 || k >= K) badIndex(k, K, "A column index");
			} else {
				k = indexPair.index1;
				if (k < 0 || k >= K) badIndex(k, K, "B row index");
				j = indexPair.index2;
				if (j < 0 || j >= J) badIndex(j, J, "B column index");
			}
			value.v = el.get();
			switch (strategy) {
				case 1:
					if (matrixA) {
						key.index1 = i/IB;
						key.index2 = k/KB;
						key.m = 0;
						value.index1 = i % IB;
						value.index2 = k % KB;
						for (int jb = 0; jb < NJB; jb++) {
							key.index3 = jb;
							context.write(key, value);
							if (DEBUG) printMapOutput(key, value);
						}
					} else {
						key.index2 = k/KB;
						key.index3 = j/JB;
						key.m = 1;
						value.index1 = k % KB;
						value.index2 = j % JB;
						for (int ib = 0; ib < NIB; ib++) {
							key.index1 = ib;
							context.write(key, value);
							if (DEBUG) printMapOutput(key, value);
						}
					}
					break;
				case 2:
					if (matrixA) {
						key.index1 = i/IB;
						key.index2 = k/KB;
						key.index3 = -1;
						value.index1 = i % IB;
						value.index2 = k % KB;
						context.write(key, value);
						if (DEBUG) printMapOutput(key, value);
					} else {
						key.index2 = k/KB;
						key.index3 = j/JB;
						value.index1 = k % KB;
						value.index2 = j % JB;
						for (int ib = 0; ib < NIB; ib++) {
							key.index1 = ib;
							context.write(key, value);
							if (DEBUG) printMapOutput(key, value);
						}
					}
					break;
				case 3:
					if (matrixA) {
						key.index1 = k/KB;
						key.index3 = i/IB;
						value.index1 = i % IB;
						value.index2 = k % KB;
						for (int jb = 0; jb < NJB; jb++) {
							key.index2 = jb;
							context.write(key, value);
							if (DEBUG) printMapOutput(key, value);
						}
					} else {
						key.index1 = k/KB;
						key.index2 = j/JB;
						key.index3 = -1;
						value.index1 = k % KB;
						value.index2 = j % JB;
						context.write(key, value);
						if (DEBUG) printMapOutput(key, value);
					}
					break;
				case 4:
					if (matrixA) {
						key.index1 = i/IB;
						key.index3 = k/KB;
						key.m = 0;
						value.index1 = i % IB;
						value.index2 = k % KB;
						for (int jb = 0; jb < NJB; jb++) {
							key.index2 = jb;
							context.write(key, value);
							if (DEBUG) printMapOutput(key, value);
						}
					} else {
						key.index2 = j/JB;
						key.index3 = k/KB;
						key.m = 1;
						value.index1 = k % KB;
						value.index2 = j % JB;
						for (int ib = 0; ib < NIB; ib++) {
							key.index1 = ib;
							context.write(key, value);
							if (DEBUG) printMapOutput(key, value);
						}
					}
					break;
			}
		}
	}
	
	/**	The job 1 partitioner class. */
	
	private static class Job1Partitioner
		extends Partitioner<Key, Value>
	{
		public int getPartition (Key key, Value value, int numPartitions) {
			int kb, ib, jb;
			switch (strategy) {
				case 1:
					kb = key.index1;
					ib = key.index2;
					jb = key.index3;
					return ((ib*JB + jb)*KB + kb) % numPartitions;
				case 2:
					ib = key.index1;
					kb = key.index2;
					return (ib*KB + kb) % numPartitions;
				case 3:
					kb = key.index1;
					jb = key.index2;
					return (jb*KB + kb) % numPartitions;
				case 4:
					ib = key.index1;
					jb = key.index2;
					return (ib*JB + jb) % numPartitions;
			}
			return 0;
		}
	}
	
	/**	The job 1 reducer class. */
	
	public static class Job1Reducer
		extends Reducer<Key, Value, NullWritable, Text>
	{
		private float[][] A;
		private float[][] B;
		private float[][] C;
		//private float[][] D;
		private int sib, skb, sjb;
		private int aRowDim, aColDim, bColDim;
		private IndexPair indexPair = new IndexPair();
		//private IntWritable el = new IntWritable();
		private FloatWritable el = new FloatWritable();
		
		public void setup (Context context) {
			init(context);
			if (DEBUG) {
				System.out.println("##### Reduce setup");
				System.out.println("   strategy = " + strategy);
				System.out.println("   I = " + I);
				System.out.println("   K = " + K);
				System.out.println("   J = " + J);
				System.out.println("   IB = " + IB);
				System.out.println("   KB = " + KB);
				System.out.println("   JB = " + JB);
			}
			//A = new int[IB][KB];
			A = new float[IB][KB];
			if (strategy == 4) {
				//C = new int[IB][JB];
			  C = new float[IB][JB];
			  //D = new float[IB][JB];
			} else {
				//B = new int[KB][JB];
			  B = new float[KB][JB];
			}
			sib = -1;
			skb = -1;
			sjb = -1;
		}
		
		private void printReduceInputKey (Key key) {
			System.out.println("##### Reduce input: key = (" + key.index1 + "," + 
				key.index2 + "," + key.index3 + "," + key.m + ")");
		}
		
		private void printReduceInputValue (Value value) {
			System.out.println("##### Reduce input: value = (" + value.index1 + "," +
				value.index2 + "," + value.v + ")");
		}
		
		private void printReduceOutput () {
			System.out.println("##### Reduce output: (" + indexPair.index1 + "," + 
				indexPair.index2 + ") " + el.get());
		}
		
		private int getDim (int blockNum, int lastBlockNum, int blockSize, 
			int lastBlockSize) 
		{
			return blockNum < lastBlockNum ? blockSize : lastBlockSize;
		}
		
		private void build (float[][] matrix, int rowDim, int colDim, 
			Iterable<Value> valueList) 
		{
			for (int rowIndex = 0; rowIndex < rowDim; rowIndex++)
				for (int colIndex = 0; colIndex < colDim; colIndex++)
					matrix[rowIndex][colIndex] = 0;
			for (Value value : valueList) {
				if (DEBUG) printReduceInputValue(value);
				matrix[value.index1][value.index2] = value.v;
			}
		}
		
		private void multiplyAndEmit (Context context, int ib, int jb)
			throws IOException, InterruptedException 
		{
			int ibase = ib*IB;
			int jbase = jb*JB;
			for (int i = 0; i < aRowDim; i++) {
				for (int j = 0; j < bColDim; j++) {
					//int sum = 0;
				  float sum = 0;
					for (int k = 0; k < aColDim; k++) {
						sum += A[i][k] * B[k][j];
					}
					if (sum != 0) {
						indexPair.index1 = ibase + i;
						indexPair.index2 = jbase + j;
						el.set(sum);
						context.write(NullWritable.get(), new Text(indexPair.index1 + "," + indexPair.index2 + "," + sum));
						//context.write(indexPair, el);
						if (DEBUG) printReduceOutput();
					}
				}
			}
		}
		
		private void multiplyAndSum (Iterable<Value> valueList) {
			for (Value value : valueList) {
				if (DEBUG) printReduceInputValue(value);
				int k = value.index1;
				int j = value.index2;
				for (int i = 0; i < aRowDim; i++) {
					/*
				  float mult = A[i][k] * value.v;
					float sum = A[i][k] + value.v;
					// check if either A[i][k], B[k][j] is 0
					if (mult == 0) {
						C[i][j] += 0;
						//D[i][j] += 0;
					} else {
						if (sum == 1) {
							C[i][j] += 0;
						} else {
							C[i][j] += sum;
						}
						D[i][j] += 1;
					}
					*/
					C[i][j] += A[i][k]*value.v;
				}
			}
		}
		
		private void emit (Context context, int ib, int jb)
			throws IOException, InterruptedException 
		{
			int ibase = ib*IB;
			int jbase = jb*JB;
			for (int i = 0; i < aRowDim; i++) {
				for (int j = 0; j < bColDim; j++) {
					//int v = C[i][j];
				  float v = C[i][j];
				  //float cnt = D[i][j];
					//if (cnt != 0) {
					if (v > threshold) {
				  //if (v != 0) {
				    indexPair.index1 = ibase + i;
						indexPair.index2 = jbase + j;
						//context.write(NullWritable.get(), 
						//		new Text(indexPair.index1 + "," + indexPair.index2 + 
						//				"," + v + "," + cnt));
						context.write(NullWritable.get(), 
						    new Text(indexPair.index1 + "," + indexPair.index2 + "," + v));
						
						//context.write(indexPair, el);
						if (DEBUG) printReduceOutput();
					}
				}
			}
		}
		
		private void zero () {
			for (int i = 0; i < aRowDim; i++)
				for (int j = 0; j < bColDim; j++)
					C[i][j] = 0;
		}	
	
		public void reduce (Key key, Iterable<Value> valueList, Context context)
			throws IOException, InterruptedException 
		{
			if (DEBUG) printReduceInputKey(key);
			int ib, kb, jb;
			switch (strategy) {
				case 1:
					ib = key.index1;
					kb = key.index2;
					jb = key.index3;
					if (key.m == 0) {
						sib = ib;
						skb = kb;
						aRowDim = getDim(ib, lastIBlockNum, IB, lastIBlockSize);
						aColDim = getDim(kb, lastKBlockNum, KB, lastKBlockSize);
						build(A, aRowDim, aColDim, valueList);
					} else {
						if (ib != sib || kb != skb) return;
						bColDim = getDim(jb, lastJBlockNum, JB, lastJBlockSize);
						build(B, aColDim, bColDim, valueList);
						multiplyAndEmit(context, ib, jb);
					}
					break;
				case 2:
					ib = key.index1;
					kb = key.index2;
					jb = key.index3;
					if (jb < 0) {
						sib = ib;
						skb = kb;
						aRowDim = getDim(ib, lastIBlockNum, IB, lastIBlockSize);
						aColDim = getDim(kb, lastKBlockNum, KB, lastKBlockSize);
						build(A, aRowDim, aColDim, valueList);
					} else {
						if (ib != sib || kb != skb) return;
						bColDim = getDim(jb, lastJBlockNum, JB, lastJBlockSize);
						build(B, aColDim, bColDim, valueList);
						multiplyAndEmit(context, ib, jb);
					}
					break;
				case 3:
					kb = key.index1;
					jb = key.index2;
					ib = key.index3;
					if (ib < 0) {
						skb = kb;
						sjb = jb;
						aColDim = getDim(kb, lastKBlockNum, KB, lastKBlockSize);
						bColDim = getDim(jb, lastJBlockNum, JB, lastJBlockSize);
						build(B, aColDim, bColDim, valueList);
					} else {
						if (kb != skb || jb != sjb) return;
						aRowDim = getDim(ib, lastIBlockNum, IB, lastIBlockSize);
						build(A, aRowDim, aColDim, valueList);
						multiplyAndEmit(context, ib, jb);
					}
					break;
				case 4:
					ib = key.index1;
					jb = key.index2;
					kb = key.index3;
					if (ib != sib || jb != sjb) {
						if (sib != -1) emit(context, sib, sjb);
						sib = ib;
						sjb = jb;
						skb = -1;
						aRowDim = getDim(ib, lastIBlockNum, IB, lastIBlockSize);
						bColDim = getDim(jb, lastJBlockNum, JB, lastJBlockSize);
						zero();
					}
					if (key.m == 0) {
						skb = kb;
						aColDim = getDim(kb, lastKBlockNum, KB, lastKBlockSize);
						build(A, aRowDim, aColDim, valueList);
					} else {
						if (kb != skb) return;
						multiplyAndSum(valueList);
					}
					break;
			}
		}
		
		public void cleanup (Context context) 
			throws IOException, InterruptedException 
		{
			if (strategy == 4 && sib != -1) emit(context, sib, sjb);
		}
		
	}
	
	
	/**	Initializes the global variables from the job context for the mapper and reducer tasks. */
	
	private static void init (JobContext context) {
		Configuration conf = context.getConfiguration();
		inputPathA = conf.get("MatrixMultiply.inputPathA");
		inputPathB = conf.get("MatrixMultiply.inputPathB");
		outputDirPath = conf.get("MatrixMultiply.outputDirPath");
		tempDirPath = conf.get("MatrixMultiply.tempDirPath");
		strategy = conf.getInt("MatrixMultiply.strategy", 0);
		R1 = conf.getInt("MatrixMultiply.R1", 0);
		R2 = conf.getInt("MatrixMultiply.R2", 0);
		I = conf.getInt("MatrixMultiply.I", 0);
		K = conf.getInt("MatrixMultiply.K", 0);
		J = conf.getInt("MatrixMultiply.J", 0);
		IB = conf.getInt("MatrixMultiply.IB", 0);
		KB = conf.getInt("MatrixMultiply.KB", 0);
		JB = conf.getInt("MatrixMultiply.JB", 0);
		NIB = (I-1)/IB + 1;
		NKB = (K-1)/KB + 1;
		NJB = (J-1)/JB + 1;
		lastIBlockNum = NIB-1;
		lastIBlockSize = I - lastIBlockNum*IB;
		lastKBlockNum = NKB-1;
		lastKBlockSize = K - lastKBlockNum*KB;
		lastJBlockNum = NJB-1;
		lastJBlockSize = J - lastJBlockNum*JB;
		useM = strategy == 1 || strategy == 4;
	}
	
	/**	Configures and runs job 1. */
	
	private static void job1 (Configuration conf)
		throws Exception
	{
	  conf.setInt("io.sort.factor", 100);
	  conf.setInt("io.sort.mb", 200);
	  conf.setInt("mapred.map.tasks", 70);
	  conf.setLong("mapred.task,timeout", 600000 * 6);
    conf.setLong("mapred.min.split.size", 512 * 1024);
    conf.setLong("mapred.max.split.size", 512 * 1024);
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		Job job = new Job(conf, "Matrix Multiply Job 1");
		job.setJarByClass(MatrixMultiply.class);
		job.setNumReduceTasks(conf.getInt("MatrixMultiply.R1", 0));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(Job1Mapper.class);
		job.setReducerClass(Job1Reducer.class);
		job.setPartitionerClass(Job1Partitioner.class);		
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);
		//job.setOutputKeyClass(IndexPair.class);
		//job.setOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(conf.get("MatrixMultiply.inputPathA")));
		FileInputFormat.addInputPath(job, new Path(conf.get("MatrixMultiply.inputPathB")));
		if (conf.getInt("MatrixMultiply.strategy", 0) == 4) {
			FileOutputFormat.setOutputPath(job, new Path(conf.get("MatrixMultiply.outputDirPath")));
		} else {
			FileOutputFormat.setOutputPath(job, new Path(conf.get("MatrixMultiply.tempDirPath")));
		}
		boolean ok = job.waitForCompletion(true);
		if (!ok) throw new Exception("Job 1 failed");
	}
	
	/**	Configures and runs job 2. */
	
	private static void job2 (Configuration conf)
		throws Exception
	{
		Job job = new Job(conf, "Matrix Multiply Job 2");
		job.setJarByClass(MatrixMultiply.class);
		job.setNumReduceTasks(conf.getInt("MatrixMultiply.R2", 0));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(Mapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(IndexPair.class);
		//job.setOutputValueClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(conf.get("MatrixMultiply.tempDirPath")));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("MatrixMultiply.outputDirPath")));
		boolean ok = job.waitForCompletion(true);
		if (!ok) throw new Exception("Job 2 failed");
	}
	
	/**	Runs a matrix multiplication job.
	 *
	 *	<p>This method is thread safe, so it can be used to run multiple concurrent
	 *	matrix multiplication jobs, provided each concurrent invocation uses a separate
	 *	configuration.
	 *
	 *	<p>The input and output files are sequence files, with key class 
	 *	MatrixMultiply.IndexPair and value class IntWritable.
	 *
	 *	@param	conf			The configuration.
	 *
	 *	@param	inputPathA		Path to input file or directory of input files for matrix A.
	 *
	 *	@param	inputPathB		Path to input file or directory of input files for matrix B.
	 *
	 *	@param	outputDirPath	Path to directory of output files for C = A*B. This directory 
	 *							is deleted if it already exists.
	 *
	 *	@param	tempDirPath		Path to directory for temporary files. A subdirectory
	 *							of this directory named "MatrixMultiply-nnnnn" is created
	 *							to hold the files that are the output of job 1 and the
	 *							input of job 2, where "nnnnn" is a random number. This 
	 *							subdirectory is deleted before the method returns. This
	 *							argument is only used for strategies 1, 2, and 3. Strategy
	 *							4 does not use a second job.
	 *
	 *	@param	strategy		The strategy: 1, 2, 3 or 4.
	 *
	 *	@param	R1				Number of reduce tasks for job 1.
	 *
	 *	@param	R2				Number of reduce tasks for job 2. Only used for strategies
	 *							1, 2, and 3.
	 *
	 *	@param	I				Row dimension of matrix A and matrix C.
	 *
	 *	@param	K				Column dimension of matrix A and row dimension of matrix B.
	 *
	 *	@param	J				Column dimension of matrix A and matrix C.
	 *
	 *	@param	IB				Number of rows per A block and C block.
	 *
	 *	@param	KB				Number of columns per A block and rows per B block.
	 *
	 *	@param	JB				Number of columns per B block and C block.
	 *
	 *	@throws	Exception
	 */
	
	public static void runJob (Configuration conf, String inputPathA, String inputPathB,
		String outputDirPath, String tempDirPath, int strategy, int R1, int R2,
		int I, int K, int J, int IB, int KB, int JB)
			throws Exception
	{
		if (conf == null) throw new Exception("conf is null");
		if (inputPathA == null || inputPathA.length() == 0)
			throw new Exception("inputPathA is null or empty");
		if (inputPathB == null || inputPathB.length() == 0)
			throw new Exception("inputPathB is null or empty");
		if (outputDirPath == null || outputDirPath.length() == 0)
			throw new Exception("outputDirPath is null or empty");
		if (tempDirPath == null || tempDirPath.length() == 0)
			throw new Exception("tempDirPath is null or empty");
		if (strategy < 1 || strategy > 4)
			throw new Exception("strategy must be 1, 2, 3 or 4");
		if (R1 < 1) throw new Exception ("R1 must be >= 1");
		if (R2 < 1) throw new Exception ("R2 must be >= 1");
		if (I < 1) throw new Exception ("I must be >= 1");
		if (K < 1) throw new Exception ("K must be >= 1");
		if (J < 1) throw new Exception ("J must be >= 1");
		if (IB < 1 || IB > I) throw new Exception ("IB must be >= 1 and <= I");
		if (KB < 1 || KB > K) throw new Exception ("KB must be >= 1 and <= K");
		if (JB < 1 || JB > J) throw new Exception ("JB must be >= 1 and <= J");
		
		FileSystem fs = FileSystem.get(conf);
		inputPathA = fs.makeQualified(new Path(inputPathA)).toString();
		inputPathB = fs.makeQualified(new Path(inputPathB)).toString();
		outputDirPath = fs.makeQualified(new Path(outputDirPath)).toString();
		tempDirPath = fs.makeQualified(new Path(tempDirPath)).toString();
		tempDirPath = tempDirPath + "/MatrixMultiply-" +
        	Integer.toString(new Random().nextInt(Integer.MAX_VALUE));
        	
		conf.set("MatrixMultiply.inputPathA", inputPathA);
		conf.set("MatrixMultiply.inputPathB", inputPathB);
		conf.set("MatrixMultiply.outputDirPath", outputDirPath);
		conf.set("MatrixMultiply.tempDirPath", tempDirPath);
		conf.setInt("MatrixMultiply.strategy", strategy);
		conf.setInt("MatrixMultiply.R1", R1);
		conf.setInt("MatrixMultiply.R2", R2);
		conf.setInt("MatrixMultiply.I", I);
		conf.setInt("MatrixMultiply.K", K);
		conf.setInt("MatrixMultiply.J", J);
		conf.setInt("MatrixMultiply.IB", IB);
		conf.setInt("MatrixMultiply.KB", KB);
		conf.setInt("MatrixMultiply.JB", JB);
		
		fs.delete(new Path(tempDirPath), true);
		fs.delete(new Path(outputDirPath), true);
		
		try {
			job1(conf);
			if (strategy != 4) job2(conf);
		} finally {
			fs.delete(new Path(tempDirPath), true);
		}
	}
	
	/**	Prints a usage error message and exits. */
	
	private static void printUsageAndExit () {
		System.err.println("Usage: MatrixMultiply [generic args] inputPathA inputPathB " +
			"outputDirPath tempDirPath strategy R1 R2 I K J IB KB JB");
		System.exit(2);
	}
	
	/**	Main program.
	 *
	 *	<p>Usage:
	 *
	 *	<p><code>MatrixMultiply [generic args] inputPathA inputPathB
	 *		outputDirPath tempDirPath strategy R1 R2 I K J IB KB JB</code>
	 *
	 *	@param	args		Command line arguments.
	 *
	 *	@throws Eception
	 */
	 
	public static void main (String[] args) 
		throws Exception
	{
	  ToolRunner.run(new MatrixMultiply(), args);
	}

  @Override
  public int run(String[] args) throws Exception {
    addOption("inputA", "a", "input path to A matrix");
    addOption("inputB", "b", "input path to B matrix");
    addOutputOption();
    addOption("strategy", null, "strategy", String.valueOf(4));
    addOption("R1", null, "number of reducer for first MR job.");
    addOption("R2", null, "number of reducer for second MR job.");
    addOption("I", null, "A matrix`s row dimension.");
    addOption("K", null, "A matrix`s col dimension.");
    addOption("J", null, "B matrix`s col dimension.");
    addOption("IB", null, "A`s row Block size.");
    addOption("KB", null, "A`s col Block size.");
    addOption("JB", null, "B`s col Block size.");
    addOption("threshold", null, "threshold for output", String.valueOf(0));
    Map<String, String> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    threshold = Float.parseFloat(getOption("threshold"));
    runJob(new Configuration(), getOption("inputA"), getOption("inputB"), 
        getOutputPath().toString(), getTempPath().toString(), 
        Integer.parseInt(getOption("strategy")),
        Integer.parseInt(getOption("R1")),
        Integer.parseInt(getOption("R2")),
        Integer.parseInt(getOption("I")),
        Integer.parseInt(getOption("K")),
        Integer.parseInt(getOption("J")),
        Integer.parseInt(getOption("IB")),
        Integer.parseInt(getOption("KB")),
        Integer.parseInt(getOption("JB")));
    return 0;
  }
}
