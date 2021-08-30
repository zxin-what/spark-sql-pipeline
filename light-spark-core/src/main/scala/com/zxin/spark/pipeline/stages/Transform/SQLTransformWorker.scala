package com.zxin.spark.pipeline.stages.Transform

import com.zxin.spark.pipeline.beans.{BaseConfig, PersistTypes}
import com.zxin.spark.pipeline.beans.transform.SQLTransformConfig
import com.zxin.spark.pipeline.config.{BusConfig, CacheConstants}
import com.zxin.spark.pipeline.constants.SysConstants
import com.zxin.spark.pipeline.stages.BaseWorker
import com.zxin.spark.pipeline.uitils.{AppUtil, DimUtils, HDFSUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SparkSession}

object SQLTransformWorker {
  def apply: SQLTransformWorker = new SQLTransformWorker()
}

class SQLTransformWorker extends BaseWorker {


  /**
   * partation、缓存、持久化、打印
   *
   * @param process ProcessItemBean
   * @param ss      SparkSession
   */
  override def process(process: BaseConfig)(implicit ss: SparkSession): Unit = {
    logger.info(s"execute sql {}", process.sql)
    val conf = BusConfig.apply.getConfig()
    val tableName = process.name
    ss.sql(process.sql).createOrReplaceTempView(tableName)
    var df = ss.table(tableName)
    // repartition
    if (process.partitions > 0) {
      df = df.repartition(process.partitions)
      df.createOrReplaceTempView(tableName)
    }
    // cache
    if (process.cache) {
      doCache(tableName)(ss)
    }
    // stored, 如果没有配置 cache, 先 cache, 再 store
    if (process.store && conf.isDebug) {
      if (!process.cache) {
        doCache(tableName)(ss)
      }
      val persistType = conf.getPersistType
      persistType match {
        case persistType if persistType == PersistTypes.hdfs.toString =>
          val path = new Path(BusConfig.apply.getConfig().persistDir, tableName).toString
          HDFSUtils.apply.saveAsCSV(df, path)(ss.sparkContext)
          logger.info(s"persist '$tableName' to hdfs '$path' success for debug.")
        case persistType if persistType == PersistTypes.hive.toString =>
          ss.table(tableName).write.mode(SaveMode.Overwrite).format("Hive").saveAsTable(s"${conf.getPersistHiveDb}.$tableName")
          logger.info(s"persist table '$tableName' to hive '${conf.getPersistHiveDb}.$tableName' success for debug.")
        case persistType if persistType == PersistTypes.local.toString =>
          val path = new Path(BusConfig.apply.getConfig().persistDir, tableName).toString
          df.write.csv(path)
          logger.info(s"persist '$tableName' to local '$path' success for debug.")
        case _ =>
      }
    }
    // show
    if (process.show > 0 && conf.getEnableShow) {
      logger.info(s"show table {}:", tableName)
      ss.table(tableName).show(process.show)
    }
    afterProcess(process)
  }

  private def doCache(tableName: String)(implicit ss: SparkSession): Unit = {
    ss.sql(s"cache table $tableName")
    CacheConstants.tables.append(tableName)
    logger.info(s"cached table '$tableName' success.")
  }
}
