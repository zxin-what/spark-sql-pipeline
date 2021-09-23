package com.zxin.spark.pipeline.stages.input

import com.alibaba.fastjson.JSON
import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.beans.input.{KafkaInputConfig, KafkaInputItem}
import com.zxin.spark.pipeline.stages.StreamBaseInputWorker
import com.zxin.spark.pipeline.stages.custom.CustomBaseInput
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import collection.JavaConverters._
import scala.collection.mutable

class KafkaInputWorker extends StreamBaseInputWorker with CustomBaseInput{
  /**
   * 加载数据
   *
   * @param bean BaseInputConfig
   * @param ss   SparkSession
   */
  override def initDf(bean: BaseConfig)(implicit ss: SparkSession): Unit = {
    val kafkaInput = bean.asInstanceOf[KafkaInputConfig]
    kafkaInput.items.asScala.foreach(kafkaInputItem => {
      ss.read.format("kafka").options(getKafkaParam(kafkaInputItem).asJava).load().createOrReplaceTempView(kafkaInput.getSourceTableName)
    })
  }

  /**
   * 合并 kafka参数
   */
  private def getKafkaParam(item: KafkaInputItem): Map[String, String] = {
    val defaultKafkaParams = getDefaultParam
    Option(item.brokers).foreach(v => defaultKafkaParams += ("bootstrap.servers" -> v))
    Option(item.autoCommit).filter(_ != null).foreach(v => defaultKafkaParams += ("enable.auto.commit" -> v.toString))
    Option(item.offersetReset).filter(v => StringUtils.isNotBlank(v)).foreach(v => defaultKafkaParams += ("auto.offset.reset" -> v))
    Option(item.groupId).foreach(v => defaultKafkaParams += ("group.id" -> v))
    Option(item.params).filter(params => CollectionUtils.isNotEmpty(params)).foreach(kvs => {
      kvs.asScala.map(_.split("=", -1)).filter(_.length == 2).map(sp => (sp(0), sp(1)))
        .foreach { case (key, v) => defaultKafkaParams += (key -> v) }
    })

    defaultKafkaParams.foreach { case (key, v) =>
      logger.info(s"kafka params: $key -> $v")
    }
    defaultKafkaParams.toMap
  }

  /**
   * kafka 默认参数
   */
  private def getDefaultParam: mutable.Map[String, String] = {
    scala.collection.mutable.Map[String, String](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer].toString,
      "value.deserializer" -> classOf[StringDeserializer].toString,
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> ("false")
    )
  }

  case class KafkaInputRecord(ip: String, port: String) extends Serializable


  /**
   * 处理任务，处理 RDD 数据，生成 SparkSQL 表
   *
   * @param rdd  要处理的数据，框架已经加载为 RDD
   * @param name 建议处理结果生这个表名
   */
  override def process(rdd: RDD[String], name: String)(implicit ss: SparkSession): Unit = {
    val transfer = rdd.map(JSON.parseObject(_, classOf[KafkaInputRecord]))
    ss.createDataFrame(transfer).createOrReplaceTempView(name)
  }
}


