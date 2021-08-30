package com.zxin.spark.pipeline.stages.input

import com.zxin.spark.pipeline.beans.input.JDBCInputConfig
import com.zxin.spark.pipeline.beans.{BaseConfig, InputTypes}
import com.zxin.spark.pipeline.stages.BaseWorker
import com.zxin.spark.pipeline.uitils.JDBCSparkUtils
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

class JdbcInputWorker extends BaseWorker {
  /**
   * 加载 jdbc 数据，参考 http://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
   * val jdbcDF = spark.read
   * .format("jdbc")
   * .option("url", "jdbc:postgresql:dbserver")
   * .option("dbtable", "schema.tablename")
   * .option("user", "username")
   * .option("password", "password")
   * .load()
   *
   * @param bean InputItemBean
   * @param ss   SparkSession
   */
  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = bean.asInstanceOf[JDBCInputConfig]
    val filtered = JDBCSparkUtils.filterValues(item)
    if (Option.apply(item.getQuery).isDefined) {
      val df = ss.sqlContext.read.format(InputTypes.jdbc.toString).options(filtered).load()
      df.createOrReplaceTempView(item.name)
    } else {
      item.dbtable.foreach { case (src, dist) =>
        filtered.put("dbtable", src)
        val df = ss.sqlContext.read.format(InputTypes.jdbc.toString).options(filtered).load()
        df.createOrReplaceTempView(item.name)
        logger.info(s"inputs, load jdbc table '$src' to Spark table '$dist' success.")
      }
    }
    afterProcess(item)
  }


}
