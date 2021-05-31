# Spark-Handson-Exercises
to define and control the partitions :
>>> rdd1 =sc.textFile("/public/retail_db/order_items",5)
>>> rdd1.take(5)
>>> filter_op.getNumPartitions
<bound method PipelinedRDD.getNumPartitions of PythonRDD[5] at collect at <stdin>:1>
>>> rdd1.getNumPartitions
<bound method RDD.getNumPartitions of /public/retail_db/order_items MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0>
Save as Picklefile is used for saving the Python files in Serialized form.
  
MapPartitions :Use Case : (If we want to add the timestamp column, or if we want to add the default value for all rows), then instead of applying via map, we can passthrough Mappartition or MappartitionwithIndex column.
  It is mainly used in the partitioned files. For Instance, if you have input data which is already partitioned and by default we have to assign the Currenttimestamp value for today's run, then we can use the Map partition.
  Difference between Map & Mappartition:
  Map is called everytime. Mappartition is called only once for that particular function.
 So, it stores the data inmemory of that function.
  
  Sample - TO generate a random sampling
  consists of following code - rdd.sample(r,f,s)
  r stands for replacement(True/False)
  f stands for fraction
  s stands for some random number generator.
  
  Cogroup - similar to full outer join
  
