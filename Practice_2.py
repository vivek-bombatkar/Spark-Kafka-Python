
Solution:
#Assumption: data is stored in avro format at location /user/tables/Log.avro

import avro.schema
from pyspark import SparkConf,SparkContext, SQLContext

conf = SparkConf().setAppName("abcd")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#read avro data file
df = sqlContext.read.format("com.databricks.spark.avro").load("/user/tables/Log.avro")

#assume data is partitioned on time, filter only last days record from partitioned data
lastDayDf = df.filter ((df['time'] = todayDate - 1) & (df['action'] = 'clicl'))

resultDf = lastDayDf.groupBy(df['user_id'],df['hotel']).agg(countDistinct(df['hotel']).alias('hotel_count')).orderBy(["hotel_count"].desc())

print (" Result : " + resultDf.show())




