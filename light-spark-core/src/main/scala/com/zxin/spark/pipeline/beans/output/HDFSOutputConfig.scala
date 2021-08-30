package com.zxin.spark.pipeline.beans.output

import com.zxin.spark.pipeline.stages.output.HdfsOutputWorker

import scala.beans.BeanProperty

class HDFSOutputConfig extends BaseOutputConfig {
  @BeanProperty
  var format: String = _
  @BeanProperty
  var path: String = _
  @BeanProperty
  var fs: String = "\u0001"
  @BeanProperty
  var srcName: String = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("format", "path", "srcName")
  }

  setWorkerClass(classOf[HdfsOutputWorker].getName)
}
