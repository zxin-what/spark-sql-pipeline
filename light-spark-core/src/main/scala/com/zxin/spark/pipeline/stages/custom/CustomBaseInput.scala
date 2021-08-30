package com.zxin.spark.pipeline.stages.custom

import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.stages.BaseWorker
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

trait CustomBaseInput extends BaseWorker {
  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {}

  /**
   * 自定义处理任务，处理 RDD 数据，生成 SparkSQL 表
   *
   * @param rdd  要处理的数据，框架已经加载为 RDD
   * @param name 建议处理结果生这个表名
   */
  def process(rdd: RDD[String], name: String)(implicit ss: SparkSession)
}
