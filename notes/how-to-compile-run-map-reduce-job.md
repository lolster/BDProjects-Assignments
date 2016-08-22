# How to compile and run a map reduce program - Hadoop MapReduce

```sh
javac -classpath $(hadoop classpath) WordCount.java
jar cf WordCount.jar WordCount*.class
hadoop jar WordCount.jar org.apache.hadoop.examples.WordCount /user/hduser/input /user/hduser/output
```
