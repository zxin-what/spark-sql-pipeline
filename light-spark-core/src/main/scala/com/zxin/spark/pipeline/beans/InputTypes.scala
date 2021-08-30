package com.zxin.spark.pipeline.beans


object InputTypes extends Enumeration {
  type InputType = Value
  val classpathFile, customHdfs, customClasspath, hdfscsv, hdfsfile, hive, jdbc, kafka, clickhouseBalance = Value
}
