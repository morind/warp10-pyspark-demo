# warp10-pyspark-demo

This project contains some examples concerning the usage of WarpScript from PySpark.
A the moment, the use of Warpscript from PySpark is performed though Java.
A wrapper 'PySparkCaller' has been developed in the 'warp10-spark' project to make it easier.

# Content 

## Warp10SparkExamples

This class contains a list of WarpScript examples in Java.
These methods are some different usages of WarpScript in Java.

## PySparkDemo

This class contains Java methods that we will call from PySpark.
These methods are used in the ipython notebook 'test.ipynb'.
To pass parameters from our notebook to Java methods, we use a dictionary in Python.
This dictionary will be casted in a Map in Java.
Then, we iterate on the list of the parameters this map contains and they will be added to the current SparkConf instance.
For more information: https://github.com/cityzendata/warp10-spark/blob/master/src/main/java/io/warp10/pyspark/PySparkCaller.java

## pyspark and the ipython notebook 'test.ipynb'

The 'pyspark' directory is located at the root of this project.
It contains the ipython notebook 'test.ipynb' to launch our example.

## test

Some units tests have beed provided to use PySparkDemo and Warp10SparkExamples in a interactive without the use of PySpark and ipython.

## How to start pyspark and ipython notebook

```pyspark --jars /XXX/warp10-pyspark-demo/build/libs/warp10-pyspark-demo.jar --files '/XXX/warp10-pyspark-demo/src/test/resources/test.mc2' --driver-class-path /XXX/warp10-pyspark-demo/build/libs/warp10-pyspark-demo.jar```

Then, the ipython notebook should be opened automatically.
Select the 'test.ipynb' file and execute.
By default, the current Warpscript examples of this project display the result on the Console (stdout).

## Build

To create the jar 'warp10-pyspark-demo.jar' use the custom task 'pack' with gradle

```gradle pack```