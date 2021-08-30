package com.zxin.spark.pipeline.beans.input

import com.zxin.spark.pipeline.stages.input.JdbcInputWorker

import scala.beans.BeanProperty
import scala.collection.JavaConversions._

class JDBCInputConfig extends BaseInputConfig {

  @BeanProperty
  var driver: String = _
  @BeanProperty
  var url: String = _
  @BeanProperty
  var user: String = _
  @BeanProperty
  var password: String = _
  @BeanProperty
  var query: String = _
  /**
   * SQL表 -> SparkSQL表
   */
  @BeanProperty
  var dbtable: java.util.Map[String, String] = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("driver", "url", "user", "password")
  }

  setWorkerClass(classOf[JdbcInputWorker].getName)

  override def getDefinedTables(): List[String] = {
    dbtable.values().toList
  }
}
