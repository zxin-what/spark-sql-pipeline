package com.zxin.spark.pipeline.function

import com.zxin.spark.pipeline.uitils.Logging
import org.apache.spark.sql.SparkSession

trait BaseUDF extends Logging {
  def setup()(implicit ss: SparkSession): Unit
}
