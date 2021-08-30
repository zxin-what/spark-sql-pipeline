package com.zxin.spark.pipeline.beans.input

import com.zxin.spark.pipeline.stages.input.HDFSCsvInputWorker

import scala.beans.BeanProperty

class HDFSCsvInputConfig extends BaseInputConfig {

  @BeanProperty
  var columns: String = _
  @BeanProperty
  var path: String = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("columns", "path")
  }

  setWorkerClass(classOf[HDFSCsvInputWorker].getName)
}
