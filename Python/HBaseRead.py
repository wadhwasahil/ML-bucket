from pyspark import SparkContext
from pyspark.sql import SQLContext
import json

sc = SparkContext(appName="HBaseInputFormat")
host = "localhost"
table = "posts"
conf = {"hbase.zookeeper.quorum": "localhost", "hbase.mapreduce.inputtable": "posts"}
keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

hbase_rdd = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter=keyConv,
        valueConverter=valueConv,
        conf=conf)
sqlContext = SQLContext(sc)
message_rdd = hbase_rdd.map(lambda x:x[1]) # message_rdd = hbase_rdd.map(lambda x:x[0]) will give only row-key
messages = message_rdd.take(1)
for message in messages:
    text = message.split("\n")
    for row in text:
        print(json.loads(row)["qualifier"])
