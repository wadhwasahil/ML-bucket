from pyspark import SparkContext, SparkConf
import json

sc = SparkContext(appName="HBaseInputFormat")
host = "localhost"
table = "posts"
conf = {"hbase.zookeeper.quorum": "localhost", "hbase.mapreduce.inputtable": "posts"}
keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

def save_record(rdd):
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    conf = {"hbase.zookeeper.quorum": "localhost",
            "hbase.mapred.outputtable": "xxxx19",
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    row_rdd = rdd.map(lambda x: x.split("\n")[0])
    datamap = row_rdd.map(lambda x: (str(json.loads(x)["row"]), [str(json.loads(x)["row"]), "p", "cats_json", "lolva"]))
    datamap.saveAsNewAPIHadoopDataset(conf=conf, keyConverter=keyConv, valueConverter=valueConv)

hbase_rdd = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter=keyConv,
        valueConverter=valueConv,
        conf=conf)

message_rdd = hbase_rdd.map(lambda x:x[1]) # message_rdd = hbase_rdd.map(lambda x:x[0]) will give only row-key
save_record(message_rdd)
messages = message_rdd.take(1)
# for message in messages:
#     text = message.split("\n")
#     for row in text:
#         print(json.loads(row))
