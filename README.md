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
  
  
   df3=df2.select(df2.order_id, df2.order_customer_id, df2.order_status, df2.withColumn("order_date1", from_unixtime((df2.order_date1/100).cast("timestamp"))))
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
  
  
  Parquet Files: cannot be read in RDD
  We can read through SQL COntext, SparkSession.
  
  Spark read & Load options :
  Spark.read.Parquet().option().load(filepath).save(save in a filename).write().bucketby("columnname", "total numbe rof Buckets).sort().partitionBy()
  Quote, escape characters - these are mainly used to ignore the , comma characters inside the Quotes.
  
  There are 4 types of Save Mode:
  SaveMode.ErrorIfExists()
  SaveMode.Append()
  SaveMod.Overwrite()
  SaveMode.Ignore()
  
  Generic File Load options:
  1. Use IgnoreCorruptfiles
  2. Use Missing files
  3. PathGlobal FIlter
  4.Recursive file lookup
  5.Modification TIme path filters - to apply the filters as Modificationtimebefore, Modificationtimeafter
 

  Sparkseesion can be imported from pyspark.sql.
  val spark = Sparksession(conf)
  
  Schema Definition:
  toDF - will use to convert to Dataframe
  SructType(StructField)
  TO cast- we have to import type 
  orders.select(orders.order_id.cast("int"), orders.order_date, )
  WithCOlumn - updating the transformation of an existing column. 
  Alias - used to convert the data into ALIAS.
  
  WholeText in Df: Its an interesting concept where the df data( all row values) is stored in all single line separated by \n.
  For example :
   df1 =spark.read.text("/user/itv000076/warehouse/itv000076_retaildb_txt.db/orders/part-00000", wholetext=True)
  withour Wholetext as True - df.collect() - works as returns list :
  Dict[Row(value= 'hello'), Row(value ='Spark')]
  With WholeText as True - df.collect() - works as :
  Dict[Row(value= 'hello'\n'Spark')]
  
  Difference between withColumn & Alias:
  withColumn - adds an another column 
  ALias - replaces the column
  
  Fixed width file format can be read only via applying substr, as all data will be read in a single column.
  
  Spark.read.table - is used to create a dataframe from Hive table.
  Spark Dataframe Operations.
  
  
  2. Find out the average revenue per day from the sales Data.
  Creation of files is completed:
  sqoop eval --connect jdbc:mysql://ms.itversity.com/retail_db --username retail_user --password itversity --e "SELECT count(1) from orders"
  sqoop import --connect jdbc:mysql://ms.itversity.com/retail_db  --username retail_user --password itversity --table orders --as-parquetfile --warehouse-dir set1/problem2 --delete-target-dir
sqoop import --connect jdbc:mysql://ms.itversity.com/retail_db --username retail_user --password itversity --table order_items --as-parquetfile --warehouse-dir set1/problem2 --delete-target-dir
  
  
  Notes from 06/19 exercise:
  In Dataframe
  1. Find the average Revenue per day.
  Code:
  orders_DF = spark.read.parquet("set1/problem2/orders/8326cc1c-ef14-48df-bfd6-4049e0474bc9.parquet")
  order_items_DF =spark.read.parquet("set1/problem2/order_items/99eb849d-acfc-4aef-aeb8-c1582157e3bd.parquet")
  orders_join = orders_DF.join(order_items_DF, orders_DF.order_id==order_items_DF.order_item_order_id, "inner")
   from pyspark.sql.functions import avg
  group_df = orders_join.groupby("order_date").agg(avg("order_item_subtotal").alias("Revenue"))
  output = group_df.write.format("csv").mode("append").save("set1/problem2/solution")
  
  withColumn is mainly used for the Adding, updating, renaming the new or existing column.
  
  
