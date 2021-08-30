package com.zxin.spark.pipeline.beans

object HDFSOutputFormats extends Enumeration {
  type HDFSOutputFormats = Value
  val csv, txt, lzo, json, parquet = Value
}
