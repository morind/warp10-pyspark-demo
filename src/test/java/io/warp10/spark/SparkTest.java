package io.warp10.spark;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.pyspark.demo.Warp10SparkExamples;
import io.warp10.spark.common.WarpScriptAbstractFunction;
import org.apache.spark.SparkConf;
import org.junit.Test;

public class SparkTest {

  private Warp10SparkExamples demo;

  public SparkTest() {
    demo = new Warp10SparkExamples();
  }

  public static void main(String... args) {
    try {
      System.out.println("testWarp10Format");
      SparkTest sparkTest = new SparkTest();
      SparkConf conf = new SparkConf().setAppName("testapp");
      //conf.set("inFile", args[0]);
      //conf.set("outFile", args[1]);
      //conf.set(WarpScriptAbstractFunction.WARPSCRIPT_FILE_VARIABLE, args[1]);
      sparkTest.testWarp10Format(conf);
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  @Test
  public void test() throws Exception {
    System.setProperty(Configuration.WARP_TIME_UNITS, "us");
    WarpConfig.setProperties((String) null);
    test(new SparkConf().setAppName("test").setMaster("local"));
  }

  @Test
  public void testWarp10Format() throws Exception {
    System.setProperty("warp10.config", "warp10.conf");
    SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
    conf.setExecutorEnv("warp10.config","warp10.conf");

    testWarp10Format(conf);
  }

  @Test
  public void testWarpScriptFile() throws Exception {
    System.setProperty("warp10.config", "warp10.conf");
    SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
    conf.set(WarpScriptAbstractFunction.WARPSCRIPT_FILE_VARIABLE, "test.mc2");
    conf.setExecutorEnv("warp10.config","warp10.conf");
    testWarpScriptFile(conf);
  }

  @Test
  public void testWarp10InputFormat() throws Exception {
    System.setProperty("warp10.config", "warp10.conf");
    SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
    conf.setExecutorEnv("warp10.config","warp10.conf");
    conf.set("token", "XXXXXX");

    testWarp10InputFormat(conf);
  }

  public void test(SparkConf conf) throws Exception {
    demo.test(conf);
  }

  public void testWarp10InputFormat(SparkConf conf) throws Exception {
    demo.testWarp10InputFormat(conf);
  }

  public void testWarp10Format(SparkConf conf) throws Exception {
    demo.testWarp10Format(conf);
  }

  public void testWarpScriptFile(SparkConf conf) throws Exception {
    demo.testWarpScriptFile(conf);
  }

}
