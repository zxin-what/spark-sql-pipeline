package com.zxin.spark.pipeline.stages.transform

import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.beans.transform.CustomTransformConfig
import com.zxin.spark.pipeline.stages.BaseWorker
import com.zxin.spark.pipeline.stages.custom.CustomBaseTransform
import com.zxin.spark.pipeline.uitils.ReflectUtils
import org.apache.spark.sql.SparkSession

object CustomTransformWorker {
  def apply: CustomTransformWorker = new CustomTransformWorker()
}

class CustomTransformWorker extends BaseWorker {
  override def process(item: BaseConfig)(implicit ss: SparkSession): Unit = {
    val config = item.asInstanceOf[CustomTransformConfig]
    ReflectUtils.apply.getInstance[CustomBaseTransform](config.clazz).doProcess(config)
    if (ss.catalog.tableExists(item.name)) {
      afterProcess(item)
    }
  }
}
