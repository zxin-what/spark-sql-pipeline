package com.zxin.spark.pipeline.beans

object NodeTypes extends Enumeration {
  type NodeTypes = Value
  val root, inputs, outputs, processes, step = Value
}
