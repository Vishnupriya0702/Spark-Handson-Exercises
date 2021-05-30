# Spark-Handson-Exercises
to define and control the partitions :
>>> rdd1 =sc.textFile("/public/retail_db/order_items",5)
>>> rdd1.take(5)
>>> filter_op.getNumPartitions
<bound method PipelinedRDD.getNumPartitions of PythonRDD[5] at collect at <stdin>:1>
>>> rdd1.getNumPartitions
<bound method RDD.getNumPartitions of /public/retail_db/order_items MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0>
Save as Picklefile is used for saving the Python files in Serialized form.
  
