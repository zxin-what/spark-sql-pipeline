package com.zxin.spark.pipeline.stages

object StageType extends Enumeration {
  type StageType = Value

  val inputs, processes, outputs = Value
}
