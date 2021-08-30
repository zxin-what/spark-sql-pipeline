package com.zxin.spark.pipeline.beans.transform

import com.zxin.spark.pipeline.stages.Transform.SQLTransformWorker

import scala.beans.BeanProperty

class SQLTransformConfig extends BaseTransformConfig {

  setWorkerClass(classOf[SQLTransformWorker].getName)

  @BeanProperty
  var dimKey: String = _

  @BeanProperty
  var allPlaceholder: String = "all"

  override protected def checkNoneIsBlank(): Unit = {
    this.step.validateNoneIsBlank("sql")
  }
}
