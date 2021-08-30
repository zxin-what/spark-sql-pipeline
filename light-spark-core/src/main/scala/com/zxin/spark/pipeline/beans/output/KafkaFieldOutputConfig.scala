package com.zxin.spark.pipeline.beans.output

import com.zxin.spark.pipeline.stages.output.KafkaFieldOutputWorker

import scala.beans.BeanProperty

class KafkaFieldOutputConfig extends KafkaOutputConfig {
  @BeanProperty
  var fs: String = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("srcName", "brokers", "topic", "fs")
  }

  setWorkerClass(classOf[KafkaFieldOutputWorker].getName)
}
