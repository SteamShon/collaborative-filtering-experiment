package com.skp.experiment.common.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.util.ReflectionUtils;



/**
 * This class re-implemented MultithreadedMapper in hadoop. most class are heavily stolen from hadoop.
 * Basically multiple threads runs map method simultaneously 
 * with static shared memory that is loaded with setup.
 * Mapper implementations using this MapRunnable must be thread-safe.
 * Be careful to use this class since this should be only called when all of following needs meet.
 * <ol>
 *  <li>Load lots of lots of data into memory in setup.</li>
 *  <li>each thread doesn`t create any conflicts on this memory.</li>
 *  <li>each map method is CPU intensive</li>
 * </ol>
 * @author doyoung yoon
 *
 * @param <K1>
 * @param <V1>
 * @param <K2>
 * @param <V2>
 */
public class MultithreadedMapMapper<K1, V1, K2, V2> extends
    MultithreadedMapper<K1, V1, K2, V2> {
  private static final Log LOG = LogFactory.getLog(MultithreadedMapper.class);
  protected Class<? extends Mapper<K1,V1,K2,V2>> mapClass;
  protected Context outer;
  protected List<MapRunner> runners;
  
  protected class SubMapRecordReader extends RecordReader<K1,V1> {
    private K1 key;
    private V1 value;
    private Configuration conf;

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void initialize(InputSplit split, 
                           TaskAttemptContext context
                           ) throws IOException, InterruptedException {
      conf = context.getConfiguration();
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      synchronized (outer) {
        if (!outer.nextKeyValue()) {
          return false;
        }
        key = ReflectionUtils.copy(outer.getConfiguration(),
                                   outer.getCurrentKey(), key);
        value = ReflectionUtils.copy(conf, outer.getCurrentValue(), value);
        return true;
      }
    }

    public K1 getCurrentKey() {
      return key;
    }

    @Override
    public V1 getCurrentValue() {
      return value;
    }
  }
  protected class SubMapRecordWriter extends RecordWriter<K2,V2> {

    @Override
    public void close(TaskAttemptContext context) throws IOException,
                                                 InterruptedException {
    }

    @Override
    public void write(K2 key, V2 value) throws IOException,
                                               InterruptedException {
      synchronized (outer) {
        outer.write(key, value);
      }
    }  
  }

  protected class SubMapStatusReporter extends StatusReporter {

    @Override
    public Counter getCounter(Enum<?> name) {
      return outer.getCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return outer.getCounter(group, name);
    }

    @Override
    public void progress() {
      outer.progress();
    }

    @Override
    public void setStatus(String status) {
      outer.setStatus(status);
    }
    
  }

  protected class MapRunner extends Thread {
    private Context subcontext;
    private Throwable throwable;

    MapRunner(Context context) throws IOException, InterruptedException {
      subcontext = new Context(outer.getConfiguration(), 
                            outer.getTaskAttemptID(),
                            new SubMapRecordReader(),
                            new SubMapRecordWriter(), 
                            context.getOutputCommitter(),
                            new SubMapStatusReporter(),
                            outer.getInputSplit());
    }

    public Throwable getThrowable() {
      return throwable;
    }

    @Override
    public void run() {
      try {
        /* this differ from Hadoop`s MultithrededMapper implementation.
         * now each thread only need to run map method. */
        while (subcontext.nextKeyValue()) {
          map(subcontext.getCurrentKey(), subcontext.getCurrentValue(), subcontext);
        }
      } catch (Throwable ie) {
        throwable = ie;
      }
    }
  }
  /**
   * Run the application's maps using a thread pool.
   */
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    /* this differ from hadoop`s MultithreadedMapper here 
     * we only need to span threads to run map() method, not setup, cleanup
     * */
    setup(context);
    
    outer = context;
    int numberOfThreads = getNumberOfThreads(context);
    mapClass = getMapperClass(context);
    if (LOG.isDebugEnabled()) {
      LOG.info("Configuring multithread runner to use " + numberOfThreads + 
                " threads");
    }
    runners =  new ArrayList<MapRunner>(numberOfThreads);
    for(int i=0; i < numberOfThreads; ++i) {
      MapRunner thread = new MapRunner(context);
      thread.start();
      runners.add(i, thread);
    }
    for(int i=0; i < numberOfThreads; ++i) {
      MapRunner thread = runners.get(i);
      thread.join();
      Throwable th = thread.throwable;
      if (th != null) {
        if (th instanceof IOException) {
          throw (IOException) th;
        } else if (th instanceof InterruptedException) {
          throw (InterruptedException) th;
        } else {
          throw new RuntimeException(th);
        }
      }
    }
    /* run cleanup only one time per task */
    cleanup(context);
  }
}
