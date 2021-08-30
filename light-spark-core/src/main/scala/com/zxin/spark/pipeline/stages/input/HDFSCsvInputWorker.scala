package com.zxin.spark.pipeline.stages.input

import com.zxin.spark.pipeline.beans.BaseConfig
import org.apache.spark.sql.SparkSession

class HDFSCsvInputWorker extends HDFSInputWorker {
  /**
   * 加载数据
   *
   * @param bean BaseInputConfig
   * @param ss   SparkSession
   */
  // TODO
  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = ???
}
