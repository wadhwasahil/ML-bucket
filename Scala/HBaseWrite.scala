package com.scryAnalytics.vopEngine

import java.util.Arrays
import java.util.ArrayList
import gate.util.GateException
import java.net.MalformedURLException
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, TableName }
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.{ TableOutputFormat, MultiTableOutputFormat }
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import com.scryAnalytics.NLPGeneric.NLPEntities
import com.vocp.ner.main.GateNERImpl
import com.scryAnalytics.NLPGeneric._
import java.util.ArrayList
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.log4j.{ Level, Logger }
import com.scryAnalytics.vopEngine.DAO.{ GateNERDAO, GenericNLPDAO, NLPEntitiesDAO }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import com.scryAnalytics.vopEngine.Configuration.VocpConfiguration
import com.scryAnalytics.vopEngine.Configuration.VOCPConstants

class NLPProcessingLog {
  var log: Logger = Logger.getLogger(classOf[NLPProcessingLog])
  log.info("Logger Initialized .....")
}

object NlpProcessing {
  val conf = VocpConfiguration.create
  val logger = new NLPProcessingLog

  @throws(classOf[Exception])
  def nlpAnnotationExtraction(conf: org.apache.hadoop.conf.Configuration, batchString: String): Int = {

    logger.log.info("In Main Object..")

    //Initializing Spark Context 
    val sc = new SparkContext(new SparkConf().setAppName("NLPAnnotationController").setMaster("local"))

    val batchId =
      if (batchString == "newbatch")
        java.lang.Long.toString(System.currentTimeMillis())
      else batchString

    conf.set("batchId", batchId)

    val inputCfs = Arrays.asList(conf.get(VOCPConstants.INPUTCOLUMNFAMILIES).split(","): _*)

    try {

      conf.set(TableInputFormat.INPUT_TABLE, conf.get(VOCPConstants.INPUTTABLE))
      conf.set(TableOutputFormat.OUTPUT_TABLE, conf.get(VOCPConstants.OUTPUTTABLE))

      val job: Job = Job.getInstance(conf, "NLPAnnotationJob")
      job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, conf.get(VOCPConstants.OUTPUTTABLE))
      job.setOutputFormatClass(classOf[MultiTableOutputFormat])

      val admin = new HBaseAdmin(conf)
      if (!admin.isTableAvailable(conf.get(VOCPConstants.OUTPUTTABLE))) {
        val tableDesc = new HTableDescriptor(TableName.valueOf(conf.get(VOCPConstants.OUTPUTTABLE)))
        admin.createTable(tableDesc)
      }

      val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])

      val processedFilteredRDD = hBaseRDD.map(x => x._2).filter { result =>
        val flag = Bytes.toString(result.getValue(Bytes.toBytes("f"),
          Bytes.toBytes("is_processed")))
        (true)
      }

      println(processedFilteredRDD.count())
      val messageRDD = processedFilteredRDD.filter { x => x != null }.map { result =>
        val message = Bytes.toString(result.getValue(Bytes.toBytes("p"),
          Bytes.toBytes("message")))
        (Bytes.toString(result.getRow()), message)

      }

      println("Number of partitions " + messageRDD.getNumPartitions)

      val pluginHome = conf.get(VOCPConstants.GATE_PLUGIN_ARCHIVE)
      val requiredNLPEntities = new ArrayList[NLPEntities]()
      requiredNLPEntities.add(NLPEntities.POS_TAGGER)
      requiredNLPEntities.add(NLPEntities.VP_CHUNKER)
      requiredNLPEntities.add(NLPEntities.NP_CHUNKER)

      val nlpGenericRDD = messageRDD.mapPartitions { iter =>
        val nlpModule = new GateGenericNLP(pluginHome, requiredNLPEntities)
        iter.map { x =>
          val nlpGenericJson = nlpModule.generateNLPEntities(x._2)
          val genericNLPObject = Utility.jsonToGenericNLP(nlpGenericJson)
          (x._1, x._2, genericNLPObject)

        }
      }

      val requiredNEREntities = new ArrayList[String]()
      requiredNEREntities.add("DRUG")
      requiredNEREntities.add("SE")
      requiredNEREntities.add("REG")
      requiredNEREntities.add("ALT_THERAPY")
      requiredNEREntities.add("ALT_DRUG")

      val nlpRDD = nlpGenericRDD.mapPartitions { iter =>
        val nerModule = new GateNERImpl(pluginHome, requiredNEREntities)
        iter.map { x =>
          val nerJson = nerModule.generateNER(x._2, Utility.objectToJson(x._3))
          val nerJsonObject = Utility.jsonToGateNer(nerJson)

          val nlpEntities: NLPEntitiesDAO = new NLPEntitiesDAO
          nlpEntities.setToken(x._3.getToken())
          nlpEntities.setSpaceToken(x._3.getSpaceToken())
          nlpEntities.setSentence(x._3.getSentence())
          nlpEntities.setSplit(x._3.getSplit())
          nlpEntities.setVG(x._3.getVG)
          nlpEntities.setNounChunk(x._3.getNounChunk)

          nlpEntities.setDRUG(nerJsonObject.getDRUG())
          nlpEntities.setREG(nerJsonObject.getREG())
          nlpEntities.setSE(nerJsonObject.getSE())
          nlpEntities.setALT_DRUG(nerJsonObject.getALT_DRUG())
          nlpEntities.setALT_THERAPY(nerJsonObject.getALT_THERAPY())
          (x._1, nlpEntities)
        }
      }

      //outputRDD.foreach(println)

      val newRDD = nlpRDD.map { k => convertToPut(k) }
      newRDD.saveAsNewAPIHadoopDataset(job.getConfiguration())
      return 0

    } catch {
      case e: MalformedURLException => {
        e.printStackTrace()
        return 1
      }
      case e: GateException =>
        {
          e.printStackTrace()
          return 1
        }

    }
  }

  def convertToPut(genericNlpWithRowKey: (String, NLPEntitiesDAO)): (ImmutableBytesWritable, Put) = {
    val rowkey = genericNlpWithRowKey._1
    val genericNLP = genericNlpWithRowKey._2
    val put = new Put(Bytes.toBytes(rowkey))
    val genCFDataBytes = Bytes.toBytes("gen")
    val nerCFDataBytes = Bytes.toBytes("ner")
    val flagCFDataBytes = Bytes.toBytes("f")

    put.add(genCFDataBytes, Bytes.toBytes("token"),
      Bytes.toBytes(Utility.objectToJson((genericNLP.getToken()))));
    put.add(genCFDataBytes, Bytes.toBytes("spaceToken"),
      Bytes.toBytes(Utility.objectToJson((genericNLP.getSpaceToken()))));
    put.add(genCFDataBytes, Bytes.toBytes("sentence"),
      Bytes.toBytes(Utility.objectToJson((genericNLP.getSentence()))));
    put.add(genCFDataBytes, Bytes.toBytes("verbGroup"),
      Bytes.toBytes(Utility.objectToJson((genericNLP.getVG()))));
    put.add(genCFDataBytes, Bytes.toBytes("split"),
      Bytes.toBytes(Utility.objectToJson((genericNLP.getSplit()))));
    put.add(genCFDataBytes, Bytes.toBytes("nounChunk"),
      Bytes.toBytes(Utility.objectToJson((genericNLP.getNounChunk()))));

    put.add(nerCFDataBytes, Bytes.toBytes("drug"),
      Bytes.toBytes(Utility.objectToJson((genericNLP.getDRUG()))))
    put.add(nerCFDataBytes, Bytes.toBytes("sideEffect"),
      Bytes.toBytes(Utility.objectToJson((genericNLP.getSE()))))
    put.add(nerCFDataBytes, Bytes.toBytes("regimen"),
      Bytes.toBytes(Utility.objectToJson((genericNLP.getREG()))))
    put.add(nerCFDataBytes, Bytes.toBytes("altTherapy"),
      Bytes.toBytes(Utility.objectToJson((genericNLP.getALT_THERAPY()))))
    put.add(nerCFDataBytes, Bytes.toBytes("altDrug"),
      Bytes.toBytes(Utility.objectToJson((genericNLP.getALT_DRUG()))))

    put.add(flagCFDataBytes, Bytes.toBytes("is_processed"),
      Bytes.toBytes("1"))
    put.add(flagCFDataBytes, Bytes.toBytes("dStatus"),
      Bytes.toBytes("0"))
    put.add(flagCFDataBytes, Bytes.toBytes("rStatus"),
      Bytes.toBytes("0"))
    put.add(flagCFDataBytes, Bytes.toBytes("adStatus"),
      Bytes.toBytes("0"))
    put.add(flagCFDataBytes, Bytes.toBytes("atStatus"),
      Bytes.toBytes("0"))
    (new ImmutableBytesWritable(Bytes.toBytes(conf.get(VOCPConstants.OUTPUTTABLE))), put)

  }

  def pipeLineExecute(args: Array[String]): Int = {

    var batchString = ""
    val usage = "Usage: NLPAnnotationController" + " -inputTable tableName -outputTable tableName" +
      " -batchId batchId / -newbatch \n"
    if (args.length == 0) {
      System.err.println(usage)
      return -1
    }

    for (i <- 0 until args.length by 2) {
      if ("-inputTable" == args(i)) {
        conf.set(VOCPConstants.INPUTTABLE, args(i + 1))
      } else if ("-outputTable" == args(i)) {
        conf.set(VOCPConstants.OUTPUTTABLE, args(i + 1))
      } else if ("-batchId" == args(i)) {
        batchString = args(i)
      } else if ("-newbatch" == args(i)) {
        batchString = "newbatch"
      } else {
        throw new IllegalArgumentException("arg " + args(i) + " not recognized")
      }
    }
    val result = nlpAnnotationExtraction(conf, batchString)
    result

  }

  def main(args: Array[String]) {
    val res = pipeLineExecute(args)
    System.exit(res)
  }

}
