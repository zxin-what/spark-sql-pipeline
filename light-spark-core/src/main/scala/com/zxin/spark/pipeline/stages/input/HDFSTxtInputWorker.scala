package com.zxin.spark.pipeline.stages.input

import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.beans.input.FileInputConfig
import com.zxin.spark.pipeline.uitils.{AppUtil, HDFSUtils}
import org.apache.spark.sql.SparkSession

object HDFSTxtInputWorker {
  def apply(): HDFSTxtInputWorker = new HDFSTxtInputWorker()
}

class HDFSTxtInputWorker extends HDFSInputWorker {
  /**
   * 加载数据
   *
   * @param bean InputItemBean
   * @param ss   SparkSession
   */
  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = bean.asInstanceOf[FileInputConfig]
    val data = HDFSUtils.apply.loadHdfsTXT(item.path, item.fs, item.nullable)(ss.sparkContext)
    AppUtil.rddToTable(data, item.fs, item.columns, item.name)
    afterProcess(item)
  }
}
