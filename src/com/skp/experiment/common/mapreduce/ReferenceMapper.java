package com.skp.experiment.common.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.apache.mahout.math.map.OpenHashMap;

import com.skp.experiment.common.parameter.DefaultOptionCreator;

/**
 * This mapper is wrapper Mapper that needs to load pair of <K, V> from sequencefile in hdfs.
 *  
 */
@SuppressWarnings("rawtypes")
public class ReferenceMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT, K extends WritableComparable, V extends Writable> 
  extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  
  public static final String REFERENCE_PATHS = ReferenceMapper.class.getName() + ".referencePaths"; 
  
  @SuppressWarnings("rawtypes")
  protected static List<OpenHashMap> references;
  protected static Object newKey;
  protected static Object newValue;
  
  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    super.setup(context);
    references = new ArrayList<OpenHashMap>();
    
    Configuration conf = context.getConfiguration();
    String referencesDir = conf.get(REFERENCE_PATHS);
    if (referencesDir != null) {
      fetchKeyValues(conf, referencesDir.split(DefaultOptionCreator.COMMA_DELIMETER));
    }
    int i = 0;
    for (OpenHashMap<K, V> item : references) {
      i++;
      for (Entry<K, V> e : item.entrySet()) {
        System.out.println(i + ":" + e.getKey() + ":" + e.getValue());
      }
    }
  } 
  
  @SuppressWarnings("unchecked")
  protected void fetchKeyValues(Configuration conf, String... pathsDir) throws IOException {
    for (int i = 0; i < pathsDir.length; i++) {
      references.add(new OpenHashMap<K, V>());
    }
    for (int i = 0; i < pathsDir.length; i++) {
      SequenceFileIterator<K, V> iter = 
          new SequenceFileIterator<K, V>(new Path(pathsDir[i]), true, conf);
      while (iter.hasNext()) {
        Pair<K, V> row = iter.next();
        references.get(i).put(ReflectionUtils.copy(conf, row.getFirst(), (K)newKey), 
            ReflectionUtils.copy(conf, row.getSecond(), (V)newValue));
      }
    }
  }
  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    super.cleanup(context);
    for (OpenHashMap<K, V> h : references) {
      h.clear();
    }
    references.clear();
  }
  
}
