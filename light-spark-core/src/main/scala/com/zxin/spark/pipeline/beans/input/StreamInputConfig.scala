package com.zxin.spark.pipeline.beans.input

import scala.beans.BeanProperty

class StreamInputConfig extends BaseInputConfig {
  @BeanProperty
  var clazz: String = _
}
