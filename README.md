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
  
  Spark Exercises Problem 1:
  MYSQL Sqoop connection :
  
   sqoop eval --connect jdbc:mysql://ms.itversity.com/retail_db --username retail_user --password itversity --e "SELECT count(1) from orders"
     sqoop import --connect jdbc:mysql://ms.itversity.com/retail_db --username retail_user --password itversity --table customers --as-textfile --fields-terminated-by '|' --warehouse-dir set1/problem1 --delete-target-dir
  sqoop import --connect jdbc:mysql://ms.itversity.com/retail_db --username retail_user --password itversity --table orders --as-textfile --fields-terminated-by '|' --warehouse-dir set1/problem1 --delete-target-dir
  
  To find the pending Orders in the city :
  
  Handling Tuples in Spark :
  Answer in RDD :
  rdd1 =sc.textFile("/user/itv000076/set1/problem1/customers")
  rdd2= sc.textFile("/user/itv000076/set1/problem1/orders")
   rdd3 = rdd2.filter(map x: x.split('|')[3]=='PENDING')
  rdd4 = rdd1.map(lambda x: x.split('|')[0], x.split('|')[6])
  rdd5 =rdd3.map(lambda x :(x.split('|')[2], x.split('|')[3]))
 rdd6 =rdd4.join(rdd5)
   rdd7 = rdd6.map(lambda x: (x[1][0], 1))
 rdd8 = rdd7.reduceByKey(lambda x,y : x+y).sortByKey()
  
  Answer in DataFrame:
val customers=spark.read.option("sep","|").option("inferSchema","true").csv("/user/cloudera/set1/problem1/customers")
val cust=customers.select("_c0","_c6").toDF("customer_id","city")
val orders=spark.read.option("sep","|").option("inferSchema","true").csv("/user/cloudera/set1/problem1/orders")
val odr=orders.select("_c2","_c3").toDF("customer_id","order_status")  
 filterdata =odr.filter(odr.order_status=='PENDING')
  joindata = filterdata.join(cust, filterdata.customer_id==cust.customer)
groupbydata = joindata.groupBy("city").count().sort("city")

  
  
  


  rdd2 = rdd1.filter(lambda x :(x.split('|')[6], x.split('|')[0]))
 rdd2 = rdd1.map(lambda (x,y) :(x.split('|')[6], 1))

  > rdd7 = rdd6.map(lambda x: (x[1][0], x[0]))

  
