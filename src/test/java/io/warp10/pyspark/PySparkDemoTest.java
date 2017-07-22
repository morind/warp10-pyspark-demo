package io.warp10.pyspark;

import io.warp10.continuum.Configuration;
import io.warp10.pyspark.demo.PySparkDemo;
import io.warp10.spark.SparkTest;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PySparkDemoTest {

  private Map<String,String> kv;
  private PySparkDemo pySparkDemo;

  @Before
  public void setup() throws Exception {
    System.setProperty("spark.master", "local");
    System.setProperty("spark.app.name", "PySparkCallerTest");
    System.setProperty(Configuration.WARP_TIME_UNITS, "us");

    kv = new HashMap<>();
    kv.put("warpscript.file", "test.mc2");
    kv.put("inFile", "hdfs://XXX/file.parquet/part-*");
    kv.put("outFile", "hdfs:///XXX/result.parquet");

    pySparkDemo = new PySparkDemo();
  }

  @Test
  public void testWarp10Format() throws Exception {
    pySparkDemo.testWarp10Format(kv);
  }

  @Test
  public void testWarpScriptFile() throws Exception {
    pySparkDemo.testWarpScriptFile(kv);
  }

}
