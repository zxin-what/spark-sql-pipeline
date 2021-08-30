package com.zxin.spark.pipeline.config

import scala.collection.mutable.ListBuffer

object CacheConstants {
  val tables: ListBuffer[String] = ListBuffer.empty[String]


  val rdds = new java.util.ArrayList[Object]()
}
