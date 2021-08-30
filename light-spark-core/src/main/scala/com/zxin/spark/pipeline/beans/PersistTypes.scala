package com.zxin.spark.pipeline.beans

object PersistTypes extends Enumeration {
  type PersistTypes = Value
  val hdfs, hive, local = Value
}
