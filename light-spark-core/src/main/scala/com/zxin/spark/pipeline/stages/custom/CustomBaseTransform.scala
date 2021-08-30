package com.zxin.spark.pipeline.stages.custom

import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.beans.transform.CustomTransformConfig
import com.zxin.spark.pipeline.stages.BaseWorker
import org.apache.spark.sql.SparkSession

trait CustomBaseTransform extends BaseWorker {
  def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {}

  /**
   * 自定义处理任务，生成 SparkSQL 表
   *
   * @param bean
   * @param ss
   */
  def doProcess(bean: CustomTransformConfig)(implicit ss: SparkSession): Unit
}
