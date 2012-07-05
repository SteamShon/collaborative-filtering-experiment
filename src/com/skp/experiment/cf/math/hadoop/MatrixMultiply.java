package com.skp.experiment.cf.math.hadoop;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.skp.experiment.cf.als.hadoop.DistributedParallelALSFactorizationJob;

public class MatrixMultiply {

	/**	True to print debugging messages. */

	private static final boolean DEBUG = true;
	private static final String DELIMETER = ",";
	private static final String LZO_CODEC_CLASS = "org.apache.hadoop.io.compress.LzoCodec"; 
	private static final Logger log = LoggerFactory.getLogger(MatrixMultiply.class);
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
		public double v;
		public void write (DataOutput out)
			throws IOException
		{
			out.writeInt(index1);
			out.writeInt(index2);
			out.writeDouble(v);
		}
		public void readFields (DataInput in)
			throws IOException
		{
			index1 = in.readInt();
			index2 = in.readInt();
			//v = in.readInt();
			v = in.readDouble();
		}
	}
	
	private static class Job1ExtMapper extends Mapper<IntWritable, VectorWritable, Key, Value> {
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
	  
    private void printMapOutput (Key key, Value value) {
      System.out.println("##### Map output: (" + key.index1 + "," + 
        key.index2 + "," + key.index3 + "," + key.m + ") (" + 
        value.index1 + "," + value.index2 + "," + value.v + ") ");
    }
    
    private void badIndex (int index, int dim, String msg) {
      System.err.println("Invalid " + msg + " in " + path + ": " + index + " " + dim);
      System.exit(1);
    }
    public void map (IntWritable rowNum, VectorWritable cols, Context context) 
        throws IOException, InterruptedException {
      Iterator<Vector.Element> iter = cols.get().iterateNonZero();
      while (iter.hasNext()) {
        Vector.Element e = iter.next();
        int i = 0, k = 0, j = 0;
        if (matrixA) {
          i = rowNum.get();
          if (i < 0 || i >= I) badIndex(i, I, "A row index");
          k = e.index();
          if (k < 0 || k >= K) badIndex(k, K, "A column index");
        } else {
          k = rowNum.get();
          if (k < 0 || k >= K) badIndex(k, K, "B row index");
          j = e.index();
          if (j < 0 || j >= J) badIndex(j, J, "B column index");
        }

        value.v = e.get();
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
	}
	
	/**	The job 1 mapper class. */
	
	private static class Job1Mapper 
		extends Mapper<IndexPair, IntWritable, Key, Value>
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
		
		public void map (IndexPair indexPair, IntWritable el, Context context)
			throws IOException, InterruptedException 
		{
			if (DEBUG) printMapInput(indexPair, el);
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
	public static class Job1Reducer 
	  extends Reducer<Key, Value, IndexPair, DoubleWritable> {
	  private double[][] A;
    private double[][] B;
    private double[][] C;
    private int sib, skb, sjb;
    private int aRowDim, aColDim, bColDim;
    private IndexPair indexPair = new IndexPair();
    private DoubleWritable el = new DoubleWritable();
    private IntWritable outKey = new IntWritable();
    private Text outValue = new Text();
    private static IndexPair threadIndexPair = new IndexPair();
    private static DoubleWritable threadel = new DoubleWritable();
    
    private static class MatMult extends Thread {
      private static double[][] A;
      private static double[][] B;
      private static int n;
      private static Context context;
      private static int ib;
      private static int jb;
      private int row;
      
      MatMult(Context ctx, double[][] a, double[][] b, int ibParam, int jbParam, int k, int r) {
        context = ctx;
        A = a;
        B = b;
        n = k;
        ib = ibParam;
        jb = jbParam;
        row = r;
        
        this.start();
      }
      public void run() {
        int ibase = ib*IB;
        int jbase = jb*JB;
        int rowID = ibase + row;
        
        for (int i = 0; i < n; i++) {
          int colID = jbase + i;
          double sum = 0;
          for (int j = 0; j < n; j++) {
            sum += A[row][j] * B[j][i];
          }
          if (sum != 0.0) {
            threadIndexPair.index1 = rowID;
            threadIndexPair.index2 = colID;
            threadel.set(sum);
            try {
              context.write(threadIndexPair, threadel);
            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        }
      }
    }
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
      A = new double[IB][KB];
      if (strategy == 4) {
        C = new double[IB][JB];
      } else {
        B = new double[KB][JB];
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
    
    private void build (Context context, double[][] matrix, int rowDim, int colDim, 
      Iterable<Value> valueList) 
    {
      context.setStatus("Build C Matrix");
      for (int rowIndex = 0; rowIndex < rowDim; rowIndex++)
        for (int colIndex = 0; colIndex < colDim; colIndex++)
          matrix[rowIndex][colIndex] = 0.0;
      for (Value value : valueList) {
        if (DEBUG) printReduceInputValue(value);
        matrix[value.index1][value.index2] = value.v;
      }
    }
    
    private void multiplyAndEmitThread(Context context, int ib, int jb) throws InterruptedException {
      MatMult threads[] = new MatMult[aRowDim];
      for (int i = 0; i < aRowDim; i++) {
        threads[i] = new MatMult(context, A, B, ib, jb, aColDim, i);
      }
      for (int i = 0; i < aRowDim; i++) {
        threads[i].join();
      }
    }
    
    private void multiplyAndEmit (Context context, int ib, int jb)
      throws IOException, InterruptedException 
    {
      context.setStatus("Multiply And Emit[" + ib + "," + jb + "]");
      log.info("aRowDim:{" + aRowDim + "} bColDim:{" + bColDim + "} aColDim: {" + aColDim + "}");
      int ibase = ib*IB;
      int jbase = jb*JB;
      int cnt = 0;
      for (int i = 0; i < aRowDim; i++) {
        for (int j = 0; j < bColDim; j++) {
          double sum = 0;
          for (int k = 0; k < aColDim; k++) {
            cnt++;
            if (cnt % 1000000 == 0) {
              context.setStatus((long)aRowDim * bColDim * aColDim + "," + aRowDim + "," + bColDim + "," + aColDim + "," + cnt);
            }
            sum += A[i][k] * B[k][j];
          }
          if (sum != 0.0) {
            indexPair.index1 = ibase + i;
            indexPair.index2 = jbase + j;
            el.set(sum);
            context.write(indexPair, el);
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
          double v = C[i][j];
          if (v != 0) {
            indexPair.index1 = ibase + i;
            indexPair.index2 = jbase + j;
            el.set(v);
            context.write(indexPair, el);
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
            build(context, A, aRowDim, aColDim, valueList);
          } else {
            if (ib != sib || kb != skb) return;
            bColDim = getDim(jb, lastJBlockNum, JB, lastJBlockSize);
            build(context, B, aColDim, bColDim, valueList);
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
            build(context, A, aRowDim, aColDim, valueList);
          } else {
            if (ib != sib || kb != skb) return;
            bColDim = getDim(jb, lastJBlockNum, JB, lastJBlockSize);
            build(context, B, aColDim, bColDim, valueList);
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
            build(context, B, aColDim, bColDim, valueList);
          } else {
            if (kb != skb || jb != sjb) return;
            aRowDim = getDim(ib, lastIBlockNum, IB, lastIBlockSize);
            build(context, A, aRowDim, aColDim, valueList);
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
            build(context, A, aRowDim, aColDim, valueList);
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
	/**	The job 1 reducer class. */
	
	public static class Job1ExtReducer
		extends Reducer<Key, Value, NullWritable, Text>
	{
		private float[][] A;
		private float[][] B;
		private float[][] C;
		private int sib, skb, sjb;
		private int aRowDim, aColDim, bColDim;
		private IndexPair indexPair = new IndexPair();
		private DoubleWritable el = new DoubleWritable();
		private IntWritable outKey = new IntWritable();
		private Text outValue = new Text();
		private boolean isIntOut = true;
		
		
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
			A = new float[IB][KB];
			if (strategy == 4) {
				C = new float[IB][JB];
			} else {
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
		
		private void build (Context context, float[][] matrix, int rowDim, int colDim, 
			Iterable<Value> valueList) 
		{
		  context.setStatus("initialize matrix build: ");
			for (int rowIndex = 0; rowIndex < rowDim; rowIndex++)
				for (int colIndex = 0; colIndex < colDim; colIndex++)
					matrix[rowIndex][colIndex] = 0;
			for (Value value : valueList) {
				if (DEBUG) printReduceInputValue(value);
				matrix[value.index1][value.index2] = (float)value.v;
			}
		}
		
		private void multiplyAndEmit (Context context, int ib, int jb)
			throws IOException, InterruptedException 
		{
			int ibase = ib*IB;
			int jbase = jb*JB;
			for (int i = 0; i < aRowDim; i++) {
			  int rowID = ibase + i;
			  for (int j = 0; j < bColDim; j++) {
				  int colID = jbase + j;
					double sum = 0;
					for (int k = 0; k < aColDim; k++) {
						sum += A[i][k] * B[k][j];
					}
					outValue.set(rowID + DELIMETER + colID + DELIMETER + sum);
	        context.write(NullWritable.get(), outValue);
				}
			}
		}
		
		private void multiplyAndSum (Context context, Iterable<Value> valueList) {
		  long cnt = 0;
			for (Value value : valueList) {
			  if (cnt++ % 1000000 == 0) {
			    context.setStatus("Cnt = " + cnt);
			  }
				if (DEBUG) printReduceInputValue(value);
				int k = value.index1;
				int j = value.index2;
				for (int i = 0; i < aRowDim; i++) {
					C[i][j] += A[i][k]*value.v;
				}
			}
		}
		
		private void emit (Context context, int ib, int jb)
			throws IOException, InterruptedException 
		{
		  context.setStatus("emitting");
			int ibase = ib*IB;
			int jbase = jb*JB;
			for (int i = 0; i < aRowDim; i++) {
			  int rowID = ibase + i;
				for (int j = 0; j < bColDim; j++) {
				  int colID = jbase + j;
				  if (isIntOut) {
				    outValue.set(rowID + DELIMETER + colID + DELIMETER + (int)Math.ceil(C[i][j]));
				  } else {
				    outValue.set(rowID + DELIMETER + colID + DELIMETER + C[i][j]);
				  }
				  context.write(NullWritable.get(), outValue);
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
						build(context, A, aRowDim, aColDim, valueList);
					} else {
						if (ib != sib || kb != skb) return;
						bColDim = getDim(jb, lastJBlockNum, JB, lastJBlockSize);
						build(context, B, aColDim, bColDim, valueList);
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
						build(context, A, aRowDim, aColDim, valueList);
					} else {
						if (ib != sib || kb != skb) return;
						bColDim = getDim(jb, lastJBlockNum, JB, lastJBlockSize);
						build(context, B, aColDim, bColDim, valueList);
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
						build(context, B, aColDim, bColDim, valueList);
					} else {
						if (kb != skb || jb != sjb) return;
						aRowDim = getDim(ib, lastIBlockNum, IB, lastIBlockSize);
						build(context, A, aRowDim, aColDim, valueList);
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
						build(context, A, aRowDim, aColDim, valueList);
					} else {
						if (kb != skb) return;
						multiplyAndSum(context, valueList);
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
	private static class Job3Mapper 
	  extends Mapper<IndexPair, DoubleWritable, NullWritable, Text> {
	  private static Text outValue = new Text();
    @Override
    protected void map(IndexPair key, DoubleWritable value, Context context) 
        throws IOException, InterruptedException {
      outValue.set(key.index1 + DELIMETER + key.index2 + value.get());
      context.write(NullWritable.get(), outValue);
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
		lastIBlockSize = Math.max(I - lastIBlockNum*IB, 0);
		lastKBlockNum = NKB-1;
		lastKBlockSize = Math.max(K - lastKBlockNum*KB, 0);
		lastJBlockNum = NJB-1;
		lastJBlockSize = Math.max(J - lastJBlockNum*JB, 0);
		useM = strategy == 1 || strategy == 4;
	}
	
	/**	Configures and runs job 1. */
	
	private static void job1 (Configuration conf)
		throws Exception
	{
		Job job = new Job(conf, "Matrix Multiply Job 1");
		
		job.setJarByClass(MatrixMultiply.class);
		job.setNumReduceTasks(conf.getInt("MatrixMultiply.R1", 0));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		if (strategy == 4) {
		  job.setOutputFormatClass(TextOutputFormat.class);
		} else {
		  job.setOutputFormatClass(SequenceFileOutputFormat.class);
		}
		
		job.setMapperClass(Job1ExtMapper.class);
		if (strategy == 4) {
		  job.setReducerClass(Job1ExtReducer.class);
		}
		else {
		  job.setReducerClass(Job1Reducer.class);
		}
		
		job.setPartitionerClass(Job1Partitioner.class);		
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);
		
    //long dfsBlockSize = 1024 * 1024 * 4;
    //int numMapTasks = 1000000;
    //job.getConfiguration().setLong("mapred.min.split.size", dfsBlockSize);
    //job.getConfiguration().setLong("mapred.max.split.size", dfsBlockSize);
    //job.getConfiguration().setInt("mapred.map.tasks", numMapTasks);
    
		//job.getConfiguration().set("mapred.child.java.opts", "-Xmx2g");
		//job.getConfiguration().setLong("mapred.task.timeout", 600000 * 6);
		//job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
		//job.getConfiguration().setInt("io.sort.mb", 200);
		//job.getConfiguration().setInt("io.sort.factor", 100);
		//job.getConfiguration().setBoolean("mapred.compress.map.output", true);
    //job.getConfiguration().set("mapred.map.output.compression.codec", LZO_CODEC_CLASS);
		
    
    if (strategy == 4) {
		  job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
		} else {
		  job.setOutputKeyClass(IndexPair.class);
	    job.setOutputValueClass(DoubleWritable.class);
		}
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
		job.setOutputValueClass(DoubleWritable.class);		
		FileInputFormat.addInputPath(job, new Path(conf.get("MatrixMultiply.tempDirPath")));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("MatrixMultiply.outputDirPath")));
		boolean ok = job.waitForCompletion(true);
		if (!ok) throw new Exception("Job 2 failed");
	}
	
	private static void job3(Configuration conf) 
	  throws Exception {
	  Job job = new Job(conf, "Matrix Multiply Job 3");
	  job.setJarByClass(MatrixMultiply.class);
	  job.setInputFormatClass(SequenceFileInputFormat.class);
	  job.setOutputFormatClass(TextOutputFormat.class);
	  job.setMapperClass(Job3Mapper.class);
	  job.setNumReduceTasks(0);
	  job.setOutputKeyClass(NullWritable.class);
	  job.setOutputValueClass(Text.class);
	  FileInputFormat.addInputPath(job, new Path(conf.get("MatrixMultiply.outputDirPath")));
	  FileOutputFormat.setOutputPath(job, new Path(conf.get("MatrixMultiply.resultPath")));
	  boolean ok = job.waitForCompletion(true);
	  if (!ok) throw new Exception("Job 3 failed");
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
    
		DistributedRowMatrix BMatrix = 
		    new DistributedRowMatrix(new Path(inputPathB), new Path(tempDirPath), J, K);
		BMatrix.setConf(new Configuration());
		DistributedRowMatrix Btranspose = BMatrix.transpose();
		
		conf.set("MatrixMultiply.inputPathA", inputPathA);
		//conf.set("MatrixMultiply.inputPathB", inputPathB);
		conf.set("MatrixMultiply.inputPathB", Btranspose.getRowPath().toString());
		conf.set("MatrixMultiply.outputDirPath", outputDirPath);
		conf.set("MatrixMultiply.resultPath", outputDirPath + ".result");
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
			if (strategy != 4) {
			  job2(conf);
			  job3(conf);
			}
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
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 13) printUsageAndExit();
		String inputPathA = otherArgs[0];
		String inputPathB = otherArgs[1];
		String outputDirPath = otherArgs[2];
		String tempDirPath = otherArgs[3];
		int strategy = 0;
		int R1 = 0;
		int R2 = 0;
		int I = 0;
		int K = 0;
		int J = 0;
		int IB = 0;
		int KB = 0;
		int JB = 0;
		try {
			strategy = Integer.parseInt(otherArgs[4]);
			R1 = Integer.parseInt(otherArgs[5]);
			R2 = Integer.parseInt(otherArgs[6]);
			I = Integer.parseInt(otherArgs[7]);
			K = Integer.parseInt(otherArgs[8]);
			J = Integer.parseInt(otherArgs[9]);
			IB = Integer.parseInt(otherArgs[10]);
			KB = Integer.parseInt(otherArgs[11]);
			JB = Integer.parseInt(otherArgs[12]);
		} catch (NumberFormatException e) {
			System.err.println("Syntax error in integer argument");
			printUsageAndExit();
		}
		MatrixMultiply.strategy = strategy;
    log.info("Running mode: {}", MatrixMultiply.strategy);
		
		runJob(conf, inputPathA, inputPathB, outputDirPath, tempDirPath, strategy,
			R1, R2, I, K, J, IB, KB, JB);
	}
}
