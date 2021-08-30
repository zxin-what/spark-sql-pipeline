package com.zxin.spark.pipeline.beans.input

import com.zxin.spark.pipeline.stages.input.KafkaInputWorker

import scala.beans.BeanProperty

class KafkaInputConfig extends StreamInputConfig {
  @BeanProperty
  var items: java.util.List[KafkaInputItem] = _

  @BeanProperty
  var sourceTableName: String = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("items")
  }

  setWorkerClass(classOf[KafkaInputWorker].getName)
}
