package com.zxin.spark.pipeline.stages.input

import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.beans.input.CustomInputConfig
import com.zxin.spark.pipeline.stages.BaseWorker
import com.zxin.spark.pipeline.stages.custom.CustomBaseInput
import com.zxin.spark.pipeline.uitils.{HDFSUtils, ReflectUtils}
import org.apache.spark.sql.SparkSession

class CustomClasspathInputWorker extends BaseWorker {

  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = bean.asInstanceOf[CustomInputConfig]
    val rdd = HDFSUtils.apply.loadClasspathFile(item.path)(ss.sparkContext)
    ReflectUtils.apply.getInstance[CustomBaseInput](item.clazz).process(rdd, item.getName)
    afterProcess(item)
  }
}
