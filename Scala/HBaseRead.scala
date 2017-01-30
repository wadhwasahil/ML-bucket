package com.scryAnalytics.NLPAnnotationController.HBaseWork

import java.util

import com.scryAnalytics.NLPGeneric.{GateGenericNLP, NLPEntities}
import com.vocp.ner.main.GateNERImpl
import com.scryAnalytics.NLPAnnotationController.DAO.{GateNERDAO, GenericNLPDAO, NLPEntitiesDAO}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat, MultiTableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.mapreduce.Job
import java.net.MalformedURLException
import gate.util.GateException

/**
  * Created by sahil on 21/11/16.
  */
object HBaseRead {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("NLPGeneric").setMaster("local"))

    try {
      val conf = HBaseConfiguration.create()
      conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
      conf.set(TableInputFormat.INPUT_TABLE, "posts")

      val job: Job = Job.getInstance(conf, "NLPAnnotationJob")
      job.setOutputFormatClass(classOf[MultiTableOutputFormat])

      val admin = new HBaseAdmin(conf)
      if (!admin.isTableAvailable("posts")) {
        val tableDesc = new HTableDescriptor(TableName.valueOf(args(1)))
        admin.createTable(tableDesc)
      }

      val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])

      val resultRDD = hBaseRDD.map(x => x._2)
      resultRDD.map(result => Bytes.toString(result.getValue(Bytes.toBytes("p"), Bytes.toBytes("message")))).foreach(println)
      } catch {
      case e: MalformedURLException => e.printStackTrace()
      case e: GateException         => e.printStackTrace()
    }
  }
  
