package com.zxin.spark.pipeline.beans.output

import com.zxin.spark.pipeline.stages.output.HiveOutputWorker

import scala.beans.BeanProperty

class HiveOutputConfig extends BaseOutputConfig {

  @BeanProperty
  var database: String = _
  @BeanProperty
  var mode: String = "overwrite"
  /**
   * src table , dist table
   */
  @BeanProperty
  var tables: java.util.Map[String, String] = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("database", "tables")
  }

  setWorkerClass(classOf[HiveOutputWorker].getName)
}
