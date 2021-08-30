package com.zxin.spark.pipeline.stages.output

import com.zxin.spark.pipeline.stages.BaseWorker
import com.zxin.spark.pipeline.uitils.KafkaUtils

trait KafkaOutputWorker extends BaseWorker {
  def sendData(iter: scala.Iterator[String], brokers: String, topic: String): Unit = {
    val toSend = iter.toList
    if (toSend.nonEmpty) {
      val producer = KafkaUtils.getKafkaProducer(brokers)
      toSend.foreach(line => KafkaUtils.sendMessage(producer, topic, line))
      producer.close()
    }
  }
}
