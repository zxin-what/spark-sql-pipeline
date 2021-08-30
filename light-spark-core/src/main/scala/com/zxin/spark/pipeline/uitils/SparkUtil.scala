package com.zxin.spark.pipeline.uitils

import com.zxin.spark.pipeline.beans.BusinessConfig
import com.zxin.spark.pipeline.config.CacheConstants
import com.zxin.spark.pipeline.constants.SysConstants
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

object SparkUtil extends Logging {
  /**
   * 初始化 SparkSession
   */
  def initSparkSession(conf: SparkConf, hiveEnabled: Boolean): SparkSession = {
    val sessionConf = SparkSession
      .builder()
      .config(conf)
    Option(hiveEnabled).filter(b => b)
      .foreach(_ => {
        sessionConf.enableHiveSupport()
        logger.info("set enableHiveSupport.")
      })
    val session = sessionConf.getOrCreate
    logger.info("Spark session initialized success.")
    session
  }

  /**
   * 获取 sparkConf
   *
   * @param appConf   appConf
   * @param variables variables
   * @return
   */
  def getSparkConf(appConf: BusinessConfig, variables: String): SparkConf = {
    val debug = appConf.isDebug
    val date = appConf.eventDate
    val conf = new SparkConf()

    /**
     * 设置appName
     */
    Option(appConf.getName)
      .filter(_ => !conf.contains("spark.app.name"))
      .foreach(_ => {
        val appName = appConf.configFile
        val name = s"$appName $date"
        conf.setAppName(name)
        logger.debug(s"set spark app name to: $name")
      })

    /**
     * master
     */
    Option(StringUtils.EMPTY)
      .filter(_ => !conf.contains("spark.master"))
      .foreach(_ => {
        val master = "local[1]"
        conf.setMaster(master)
        System.setProperty("HADOOP_USER_NAME", "hadoop")
        logger.warn(s"set spark master url to '$master' by default.")
        conf.set("spark.sql.warehouse.dir", "file:///spark-warehouse")
        conf.set("spark.testing.memory", "471859200")
      })

    Option(debug).filter(d => d).foreach(d => {
      conf.set("spark.logConf", "true")
      logger.debug("show spark conf as INFO log")
      conf.set("spark.logLineage", "true")
      logger.debug("show rdd lineage in log")
    })

    Option(debug).filter(d => !d).foreach(d => {
      conf.set("spark.ui.showConsoleProgress", "false")
      logger.debug("disabled spark progress info")
    })

    conf.set("spark.sql.crossJoin.enabled", "true")
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
    logger.info("enabled spark sql cross join")

    /**
     * 设置自定义参数
     */
    SysConstants.SYS_SPARK_CONFIG.foreach { case (key, v) =>
      logger.info(s"Spark config set - $key=$v.")
      conf.set(key, v)
    }
    conf
  }

  private[spark] def uncacheData()(implicit ss: SparkSession): Unit = {
    CacheConstants.tables.foreach { t =>
      ss.sql(s"uncache table $t")
      logger.info(s"uncached table '$t'.")
    }
    CacheConstants.rdds.foreach(rdd => rdd.asInstanceOf[RDD[Object]].unpersist())
  }
}
