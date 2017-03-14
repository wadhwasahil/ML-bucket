from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans, KMeansModel
from gensim import models
from nltk.tokenize import TweetTokenizer
from math import sqrt
from numpy import array
import json
import traceback

tokenizer = TweetTokenizer()
file_name = "small_text.txt"
dir_name = "doc2vec_models/"
input_table = output_table = "forumPosts"


def is_word(word):
    for char in word:
        if char.isalpha() or char.isdigit():
            return True
    return False


def get_segment(text):
    try:
        segment = "{}"
        segments = []
        flag = 0
        for line in text:
            json_txt = json.loads(line)
            rowkey = str(json_txt["row"])
            if json_txt["qualifier"] == "message":
                segment = json_txt["value"]
            if json_txt["qualifier"] == "flag":
                flag = json_txt["value"]
        if flag != 0:
           return [(None, None)]
        if flag != 0 and flag is not None:
           return [(None, None)]
        if segment == "" or rowkey == "":
            return [(None, None)]
        segments.append((rowkey, segment))
        if len(segments) == 0:
            return [(None, None)]
        return segments
    except Exception as e:
        traceback.print_exc()
        return [(None, None)]


def filter_rows(row):
    for i in range(len(row)):
        if row[i] is None:
            return False
    return True


def get_vector(iter):
    print("Loading model.............")
    doc2vec_model = models.Doc2Vec.load(dir_name + "model_1")
    print("Model loaded..............")
    for row in iter:
        words = [word for word in tokenizer.tokenize(row[1]) if is_word(word)]
        yield array(doc2vec_model.infer_vector(words))


def transform(row):
    tuple1 = (output_table, [row[0], "data", "flag", "1"])
    tuple2 = (output_table, [row[0], "data", "cluster", str(row[1])])
    return ([tuple1, tuple2])


def save_record(rdd):
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    conf = {"hbase.zookeeper.quorum": "localhost",
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    rdd.saveAsNewAPIHadoopDataset(conf=conf, keyConverter=keyConv, valueConverter=valueConv)


sc = SparkContext(appName="Questions Clustering")
sc.addFile(file_name)
ssc = StreamingContext(sc, 1)

# rdd = sc.textFile("file:///" + os.path.join(os.getcwd(), file_name))
host = "localhost"
conf = {"hbase.mapreduce.inputtable": input_table}
keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
hbase_rdd = sc.newAPIHadoopRDD(
    "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
    "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
    "org.apache.hadoop.hbase.client.Result",
    keyConverter=keyConv,
    valueConverter=valueConv,
    conf=conf)
hbase_rdd = hbase_rdd.map(lambda x: x[1]).map(lambda x: x.split("\n"))
rdd = hbase_rdd.flatMap(lambda x: get_segment(x))
rdd = rdd.filter(lambda x: filter_rows(x))
rdd.cache()
parsedData = rdd.mapPartitions(lambda iter: get_vector(iter))
row_rdd = rdd.map(lambda x: x[0])
print("Training K-Means model................")
clusters = KMeans.train(parsedData, 2, maxIterations=10, initializationMode="random")
print("K-Means model trained.................")


def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x ** 2 for x in (point - center)]))


WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

predictions = parsedData.map(lambda x: clusters.predict(x))
final_rdd = row_rdd.zip(predictions).flatMap(lambda x: transform(x))
save_record(final_rdd)
