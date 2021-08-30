package com.zxin.spark.pipeline.beans.input

import com.zxin.spark.pipeline.stages.input.CustomFileInputWorker

class CustomHDFSInputConfig extends CustomInputConfig {
  setWorkerClass(classOf[CustomFileInputWorker].getName)
}
