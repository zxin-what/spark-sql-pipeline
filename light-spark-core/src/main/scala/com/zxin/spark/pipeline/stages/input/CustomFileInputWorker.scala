package com.zxin.spark.pipeline.stages.input

import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.beans.input.CustomInputConfig
import com.zxin.spark.pipeline.stages.custom.CustomBaseInput
import com.zxin.spark.pipeline.uitils.ReflectUtils
import org.apache.spark.sql.SparkSession

class CustomFileInputWorker extends HDFSInputWorker {
  /**
   * 加载数据
   *
   * @param bean BaseInputConfig
   * @param ss   SparkSession
   */
  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = bean.asInstanceOf[CustomInputConfig]
    // 如果数据 path 不存在，且配置定义该数据允许为空，则给一个空的 rdd
    val rdd = loadAsText(item.path, item.nullable)(ss.sparkContext)

    ReflectUtils.apply.getInstance[CustomBaseInput](item.clazz).process(rdd, item.getName)
    afterProcess(item)
  }
}
