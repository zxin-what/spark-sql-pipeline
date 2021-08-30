package com.zxin.spark.pipeline.beans

import java.util

import scala.beans.BeanProperty

class EnvConfig extends Serializable {
  @BeanProperty
  var spark: java.util.List[String] = new util.ArrayList[String]()
}
