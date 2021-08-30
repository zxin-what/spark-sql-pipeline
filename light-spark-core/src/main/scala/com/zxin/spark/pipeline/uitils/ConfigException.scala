package com.zxin.spark.pipeline.uitils

class ConfigException extends Throwable with Logging {
  def this(msg: String, e: Throwable) {
    this()
    logger.error(msg, e)
  }

  def this(msg: String) {
    this()
    logger.error(msg)
  }
}
