package com.skp.experiment.common.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * reference data: 
 * file format can be SequenceFile, TestFile
 * 
 * @author skplanet
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public class MemoryIntensiveMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  public static final String REFERENCE_DATA = MemoryIntensiveMapper.class.getName() + ".referenceData";
  private Path referencePath;
  
  /**
   * in setup phase, read reference data, reference data can be either file or directory 
   * */
  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    String referenceDir = conf.get(REFERENCE_DATA);
    if (referenceDir != null) {
      referencePath = new Path(referenceDir);
      
    }
  }
  
}
