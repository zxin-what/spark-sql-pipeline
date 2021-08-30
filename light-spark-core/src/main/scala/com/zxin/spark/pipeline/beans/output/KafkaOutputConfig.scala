package com.zxin.spark.pipeline.beans.output

import scala.beans.BeanProperty

class KafkaOutputConfig extends BaseOutputConfig {
  @BeanProperty
  var srcName: String = _
  @BeanProperty
  var brokers: String = _
  @BeanProperty
  var topic: String = _
}
