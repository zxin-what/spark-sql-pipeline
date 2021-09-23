package com.zxin.spark.pipeline.beans.transform

import com.zxin.spark.pipeline.stages.transform.CustomTransformWorker

class CustomTransformConfig extends BaseTransformConfig {

  setWorkerClass(classOf[CustomTransformWorker].getName)

  override protected def checkNoneIsBlank(): Unit = {
    this.step.validateNoneIsBlank("clazz")
  }
}
