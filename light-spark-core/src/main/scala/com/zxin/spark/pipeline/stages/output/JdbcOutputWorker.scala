package com.zxin.spark.pipeline.stages.output

import java.sql.DriverManager

import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.beans.output.JDBCOutputConfig
import com.zxin.spark.pipeline.stages.BaseWorker
import com.zxin.spark.pipeline.uitils.JDBCSparkUtils
import org.apache.commons.collections4.CollectionUtils
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

class JdbcOutputWorker extends BaseWorker {
  override def process(config: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = config.asInstanceOf[JDBCOutputConfig]
    // 执行前置sql逻辑，一般为删除操作
    Option(item.preSQL).filter(pre => CollectionUtils.isNotEmpty(pre)).foreach(_ => {
      executePreSQL(item)
    })
    val filterd = JDBCSparkUtils.filterValues(item)
    if (item.opts != null && item.opts.nonEmpty) {
      for ((key, v) <- item.opts) {
        filterd.put(key, v)
        logger.info(s"use jdbc opts - $key -> $v")
      }
    }
    filterd.remove("tables")
    item.tables.foreach { case (src, dist) =>
      logger.info(s"jdbc output, start save '$src' to '$dist'...")
      val t1 = System.currentTimeMillis()
      val dfWriter = ss.table(src).write.mode(item.mode).format("jdbc")
      filterd.put("dbtable", dist)
      dfWriter.options(filterd).save()
      logger.info(s"jdbc output, save '$src' to '$dist' success cost ${System.currentTimeMillis() - t1}.")
    }
  }

  /**
   * 前置 SQL 逻辑
   *
   * @param item JDBC 配置
   */
  def executePreSQL(item: JDBCOutputConfig): Unit = {
    logger.info(s"connect url: ${item.url}")
    Class.forName(item.driver)
    val conn = DriverManager.getConnection(item.url, item.user, item.password)
    item.preSQL.foreach(sql => {
      logger.info(s"start execute preSQL: $sql")
      val stmt = conn.prepareStatement(sql)
      val result = stmt.executeUpdate()
      logger.info(s"execute preSQL success, result: $result.")
      stmt.close()
    })
    conn.close()
  }
}
