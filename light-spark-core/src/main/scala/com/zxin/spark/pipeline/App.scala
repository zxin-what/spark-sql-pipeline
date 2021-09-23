package com.zxin.spark.pipeline

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import com.zxin.spark.pipeline.beans.BusinessConfig
import com.zxin.spark.pipeline.config.{BusConfig, CacheConstants}
import com.zxin.spark.pipeline.constants.{AppConstants, SysConstants}
import com.zxin.spark.pipeline.stages.Pipeline
import com.zxin.spark.pipeline.uitils.{Logging, SparkUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import collection.JavaConverters._
import scala.reflect.io.File

/**
 * Spark主程序入口
 */
object App extends Logging {

  def main(args: Array[String]): Unit = {
    val appConfig = BusConfig.apply.parseConfigFile("example-sqlserver.yaml")
    logger.info(s"load config success, event date is ${appConfig.eventDate}")
    val confMap: java.util.Map[String, String] = AppConstants.variables.asJava
    val strConf = JSON.toJSONString(confMap, new Array[SerializeFilter](0))

    val sparkConf = SparkUtil.getSparkConf(appConfig, strConf)
    implicit val sparkSession: SparkSession = SparkUtil.initSparkSession(sparkConf, hiveEnabled = false)
    Pipeline.runTask(appConfig)
    cleanUp()
    logger.info(s"context exit success.")
  }

  private def cleanUp(): Unit = {
    AppConstants.variables.clear()
    SysConstants.SYS_SPARK_CONFIG.clear()
    SysConstants.SYS_DEFAULT_VARIABLES.clear()
    SysConstants.SYS_DEFINED_TABLES.clear()
    CacheConstants.rdds.clear()
    CacheConstants.tables.clear()
  }
}
