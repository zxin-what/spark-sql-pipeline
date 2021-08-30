package com.zxin.spark.pipeline.beans.input

import scala.beans.BeanProperty

class KafkaInputItem extends Serializable {
  @BeanProperty
  var brokers: String = _
  @BeanProperty
  var topic: String = _
  @BeanProperty
  var groupId: String = _
  @BeanProperty
  var offersetReset: String = "earliest"
  @BeanProperty
  var autoCommit: java.lang.Boolean = false
  @BeanProperty
  var commitOffset: java.lang.Boolean = true
  /**
   * kafkaParams
   */
  @BeanProperty
  var params: java.util.List[String] = _
}
