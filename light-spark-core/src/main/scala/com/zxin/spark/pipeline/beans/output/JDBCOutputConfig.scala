package com.zxin.spark.pipeline.beans.output

import com.zxin.spark.pipeline.stages.output.JdbcOutputWorker

import scala.beans.BeanProperty

class JDBCOutputConfig extends BaseOutputConfig {
  @BeanProperty
  var driver: String = _
  @BeanProperty
  var url: String = _
  @BeanProperty
  var user: String = _
  @BeanProperty
  var password: String = _
  @BeanProperty
  var tables: java.util.Map[String, String] = _
  @BeanProperty
  var mode: String = "append"
  @BeanProperty
  var opts: java.util.Map[String, String] = _
  @BeanProperty
  var preSQL: java.util.List[String] = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("driver", "url", "user", "password", "tables")
  }

  setWorkerClass(classOf[JdbcOutputWorker].getName)
}
