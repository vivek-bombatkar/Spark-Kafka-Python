
import avro.schema
from pyspark import SparkConf,SparkContext, SQLContext
from pyspark.streaming.kafka import KafkaUtils

conf = SparkConf().setAppName("abcd")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


directKafkaStream = KafkaUtils.createDirectStream(ssc, "<topic_name>", {"metadata.broker.list": brokers})


lines = KafkaUtils.createStream(ssc, zookeeperQuorum, group, topicMap)


destination = lines.map(lambda x : x["destination"])

#wo word count logic on destination for window of 10 minutes repeating each 1 minute.
destinationCounts = destination.map(lambda x : (x, 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, Minutes(10), Minutes(1))

# exchange the position of key value & sort the result by count descending
redultRDD = destinationCounts.map(lambda (x,y) : (y,x)).sortByKey("false")

print ("Result : " + redultRDD.take(10))
