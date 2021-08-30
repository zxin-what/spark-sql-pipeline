package com.zxin.spark.pipeline.uitils

import org.slf4j.LoggerFactory

trait Logging extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName.stripSuffix("$"))
}
