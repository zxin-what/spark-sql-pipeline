package com.zxin.spark.pipeline.stages

import com.alibaba.fastjson.JSON
import com.zxin.spark.pipeline.beans.{BaseConfig, PersistTypes}
import com.zxin.spark.pipeline.config.{BusConfig, CacheConstants}
import com.zxin.spark.pipeline.uitils.{HDFSUtils, Logging}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

trait BaseWorker extends Logging {
  protected var variables: java.util.HashMap[String, String] = _

  def process(bean: BaseConfig)(implicit ss: SparkSession): Unit

  protected def initVariables()(implicit ss: SparkSession): Unit = {
    val str = ss.sparkContext.getConf.get("com.zxin.spark.pipeline.variables")
    variables = JSON.parseObject(str, classOf[java.util.HashMap[String, String]])
  }

  protected def getVariableStr()(implicit ss: SparkSession): String = {
    ss.sparkContext.getConf.get("com.zxin.spark.pipeline.variables")
  }

  /**
   * spark table 转 RDD
   *
   * @param tableName
   * @param ss
   * @return
   */
  def getRDDByTable(tableName: String)(implicit ss: SparkSession): RDD[Row] = {
    ss.table(tableName).rdd
  }

  protected def afterProcess(process: BaseConfig)(implicit ss: SparkSession): Unit = {

  }
}
