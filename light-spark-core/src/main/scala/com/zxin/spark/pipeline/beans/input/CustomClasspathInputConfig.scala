package com.zxin.spark.pipeline.beans.input

import com.zxin.spark.pipeline.stages.input.CustomClasspathInputWorker

class CustomClasspathInputConfig extends CustomInputConfig {
  setWorkerClass(classOf[CustomClasspathInputWorker].getName)
}
