package com.zxin.spark.pipeline.beans.transform

import com.zxin.spark.pipeline.beans.{BaseConfig, NodeTypes}

import scala.beans.BeanProperty

class BaseTransformConfig extends BaseConfig {
  tag = NodeTypes.processes.toString
  @BeanProperty
  var step: BaseConfig = _
  @BeanProperty
  var clazz: String = _

  def this(sql: String) {
    this
    this.sql = sql
  }
}
