package io.warp10.pyspark.demo;

import io.warp10.continuum.Configuration;
import io.warp10.hadoop.Warp10InputFormat;
import io.warp10.spark.WarpScriptFlatMapFunction;
import io.warp10.spark.WarpScriptFunction;
import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Warp10SparkExamples {

  public static void main(String... args) {

    try {
      //
      // Otherwise use JVM option -Dspark.master=XXXX
      //
      System.setProperty("spark.master", "local");
      System.setProperty("spark.app.name", "Warp10SparkExamples");
      System.setProperty(Configuration.WARP_TIME_UNITS, "us");

      Map<String,String> kv = new HashMap<>();
      kv.put("warpscript.file", "test.mc2");
      kv.put("inFile", "hdfs://XXX/file.parquet/part-*");
      kv.put("outFile", "hdfs:///XXX/result.parquet");

      Warp10SparkExamples demo = new Warp10SparkExamples();
      SparkConf conf = new SparkConf().setAppName("Warp10SparkExamples");
      demo.testWarp10Format(conf);
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }

  }

  /**
   * Basic test in WarpScript
   *
   * @param conf
   * @throws Exception
   */
  public void test(SparkConf conf) throws Exception {

    JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

    List<String> list = new ArrayList<String>();

    for (int i = 0; i < 1280; i++) {

      list.add(Long.toString(i));

    }

    JavaRDD<String> lines = sc.parallelize(list, 10);

    lines = lines.keyBy(new WarpScriptFunction<>("0 1 SUBSTRING")).flatMap(new WarpScriptFlatMapFunction<>("SNAPSHOT [ SWAP ]"));

    System.out.println(lines.collect());

  }

  /**
   * Use Warp10Format and use some functions (mapper.pow,...)
   *
   * @param conf
   * @throws Exception
   */
  public void testWarp10Format(SparkConf conf) throws Exception {

    JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

    System.out.println("testWarp10Format");

    // Create input metrics

    List<String> list = new ArrayList<String>();

    list.add("1476886686000000// test.data{id=0} 10");
    list.add("1476886687000000// test.data{id=1} 20");
    list.add("1476886688000000// test.data{id=1} 30");
    list.add("1476886689890000// test.data{id=1} 40");
    list.add("1476886690000000// test.data{id=1} 50");
    list.add("1476886691000000// test.data{id=2} 60");
    list.add("1476886692000000// test.data{id=2} 70");
    list.add("1476886693000000// test.data{id=2} 80");
    list.add("1476886694000000// test.data{id=2} 90");
    list.add("1476886695000000// test.data{id=2} 100");

    JavaRDD<String> lines = sc.parallelize(list);

    //lines = lines.keyBy(new WarpScriptFunction<>("0 1 SUBSTRING")).flatMap(new WarpScriptFlatMapFunction<> ("SNAPSHOT [ SWAP ] LIST-> DROP"));
    //JavaRDD<byte[]> exec = lines.flatMap(new WarpScriptFlatMapFunction<>("PARSE WRAPRAW"));

    JavaRDD<String> exec = lines.flatMap(new WarpScriptFlatMapFunction<>("PARSE [ SWAP 2.0 mapper.pow 0 0 0 ] MAP <% DROP TOSTRING %> LMAP"));
    //JavaRDD<String> exec2 = exec.flatMap(new WarpScriptFlatMapFunction<>("UNWRAP [ SWAP [ '~.*' ] reducer.sum ] REDUCE <% DROP TOSTRING %> LMAP"));
    System.out.println(exec.collect());

  }

  /**
   * How to use WarpScript file
   *
   * @param conf
   * @throws Exception
   */
  public void testWarpScriptFile(SparkConf conf) throws Exception {

    JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
    String warpScriptFile = conf.get(WarpScriptAbstractFunction.WARPSCRIPT_FILE_VARIABLE);

    System.out.println("testWarpScriptFile");

    // Create input metrics
    List<String> list = new ArrayList<String>();

    list.add("1476886686000000// test.data{id=0} 10");
    list.add("1476886687000000// test.data{id=1} 20");
    list.add("1476886689000000// test.data{id=1} 30");
    list.add("1476886692000000// test.data{id=1} 40");
    list.add("1476886696000000// test.data{id=1} 50");
    list.add("1476886701000000// test.data{id=2} 60");
    list.add("1476886707000000// test.data{id=2} 70");
    list.add("1476886714000000// test.data{id=2} 80");
    list.add("1476886722000000// test.data{id=2} 90");
    list.add("1476886731000000// test.data{id=2} 100");

    JavaRDD<String> lines = sc.parallelize(list, 10);

    //
    // /var/tmp/test.mc2 content => PARSE WRAPRAW LIST-> DROP
    //
    JavaRDD<byte[]> exec = lines.flatMap(new WarpScriptFlatMapFunction<>("@" + warpScriptFile));
    JavaRDD<String> result = exec.map(val -> SparkUtils.GTSDump(val, true));

    System.out.println(result.collect());

  }

  /**
   * The Warp10 Input Format (HDFS like) - Fetch data from a Warp 10 platform (distributed only)
   *
   * @param conf
   * @throws Exception
   */
  public void testWarp10InputFormat(SparkConf conf) throws Exception {

    JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

    sc.hadoopConfiguration().set("warp10.fetcher.protocol","http");
    sc.hadoopConfiguration().set("warp10.fetcher.fallbacks","localhost");
    sc.hadoopConfiguration().set("http.header.now","X-CityzenData-Now");
    sc.hadoopConfiguration().set("http.header.timespan","X-Warp10-Timespan");
    sc.hadoopConfiguration().set("warp10.fetcher.port","8881");
    sc.hadoopConfiguration().set("warp10.fetcher.path","/api/v0/sfetch");
    sc.hadoopConfiguration().set("warp10.splits.endpoint","https://warp.cityzendata.net/api/v0/splits");
    sc.hadoopConfiguration().set("warp10.fetch.timespan","-10");
    sc.hadoopConfiguration().set("warp10.http.connect.timeout","10000");
    sc.hadoopConfiguration().set("warp10.http.read.timeout","10000");
    sc.hadoopConfiguration().set("warp10.max.splits","10");

    sc.hadoopConfiguration().set("warp10.fetcher.fallbacksonly", "true");

    sc.hadoopConfiguration().set("warp10.splits.token", conf.get("token"));
    sc.hadoopConfiguration().set("warp10.splits.selector", "~.*{}");
    sc.hadoopConfiguration().set("warp10.fetch.now", "1444000000000000");
    sc.hadoopConfiguration().set("warp10.fetch.timespan", "600000000000000");

    System.out.println("testWarp10InputFormat");

    JavaPairRDD<Text, BytesWritable> inputRDD = sc.newAPIHadoopFile("test", Warp10InputFormat.class, Text.class, BytesWritable.class, sc.hadoopConfiguration());

    //JavaRDD<String> lines = inputRDD.values().map(new WarpScriptFunction<>("UNWRAP VALUES"));
    JavaRDD < String > lines = inputRDD.values().map(value -> SparkUtils.GTSDump(value.getBytes(), true));

    System.out.println(lines.collect());

  }





}
