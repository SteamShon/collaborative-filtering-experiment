package com.skp.experiment.cf.math.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DistributedRowMatrixWriter;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;

import com.skp.experiment.common.MahoutTestCase;

public class DistributedRowMatrixTestUtil extends MahoutTestCase {
  public static final String TEST_PROPERTY_KEY = "test.property.key";
  public static final String TEST_PROPERTY_VALUE = "test.property.value";
  
  public static Configuration createInitialConf() {
    Configuration initialConf = new Configuration();
    initialConf.set(TEST_PROPERTY_KEY, TEST_PROPERTY_VALUE);
    return initialConf;
  }

  private static void deleteContentsOfPath(Configuration conf, Path path) throws Exception {
    FileSystem fs = path.getFileSystem(conf);
    
    FileStatus[] statuses = fs.listStatus(path);
    for (FileStatus status : statuses) {
      fs.delete(status.getPath(), true);
    }    
  }
    

  public DistributedRowMatrix convert2DistributedRowMatrix(double[][] m, 
                                                           int numRows, 
                                                           int numCols,
                                                           Configuration conf)
    throws IOException {
    Path baseTmpDirPath = getTestTempDirPath();
    Matrix matrix = new DenseMatrix(m);
    DistributedRowMatrixWriter.write(baseTmpDirPath, conf, matrix);
    
    DistributedRowMatrix result = new DistributedRowMatrix(baseTmpDirPath, 
                                                           getTestTempDirPath("temp"), 
                                                           numRows, 
                                                           numCols);
    result.setConf(conf);
    return result;
  }
  
}
