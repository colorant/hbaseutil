package org.apache.hadoop.hbase;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDataGenerate {

  static final Log LOG = LogFactory.getLog(TestDataGenerate.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin = null;
  private static Configuration conf = null;

  
/*  this unit test part of code is for debugging usage. */
  
  @BeforeClass
  public static void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster(1);
    conf.setInt("hbase.client.retries.number", 1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  

  /**
   * do test here.
   * @throws IOException
   */
  @Test
  public void testForDebug() throws IOException {
    String[] fakeArgs = {
        "--cfnum=1",
        "--colnum=12",
        "--rownum=100",
        "--table=testDataTable",
        "--regions=4",
        "--colsformat=s,i:0:10,l,t:12:16,s,s,s,s,s,s,s,s",
        "--usebulkload"
    };
   
    try {
      GenerateTestTable.testmain(fakeArgs, conf);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
