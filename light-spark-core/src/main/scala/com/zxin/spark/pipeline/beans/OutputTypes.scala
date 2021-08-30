package com.zxin.spark.pipeline.beans

object OutputTypes extends Enumeration {
  type OutputType = Value
  val kafkaJson, kafkaField, hdfsfile, hive, jdbc, clickhouseBalance = Value
}
