package io.warp10.pyspark.demo;

import io.warp10.pyspark.PySparkCaller;
import org.apache.spark.SparkConf;

import java.util.HashMap;
import java.util.Map;

public class PySparkDemo {

  public static void main(String... args) throws Exception {
    //
    // Otherwise use JVM option -Dspark.master=XXXX
    //
    System.setProperty("spark.master", "local");
    System.setProperty("spark.app.name", "PySparkDemo");
    System.setProperty("warp.timeunits", "us");

    Map<String,String> kv = new HashMap<>();
    kv.put("warpscript.file", "test.mc2");
    kv.put("inFile", "hdfs://XXX/file.parquet/part-*");
    kv.put("outFile", "hdfs:///XXX/result.parquet");

    PySparkDemo pySparkDemo = new PySparkDemo();
    pySparkDemo.testWarp10Format(kv);
  }

  public void testWarp10Format(Map<String,String> kv) throws Exception {
    PySparkCaller pySparkCaller = new PySparkCaller();
    SparkConf sparkConf = pySparkCaller.getConf(kv);
    Warp10SparkExamples examples = new Warp10SparkExamples();
    examples.testWarp10Format(sparkConf);
  }

  public void testWarpScriptFile(Map<String,String> kv) throws Exception {
    PySparkCaller pySparkCaller = new PySparkCaller();
    SparkConf sparkConf = pySparkCaller.getConf(kv);
    Warp10SparkExamples examples = new Warp10SparkExamples();
    examples.testWarpScriptFile(sparkConf);
  }

}
