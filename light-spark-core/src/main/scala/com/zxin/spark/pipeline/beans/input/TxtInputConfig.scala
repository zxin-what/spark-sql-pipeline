package com.zxin.spark.pipeline.beans.input

import com.zxin.spark.pipeline.stages.input.HDFSTxtInputWorker

class TxtInputConfig extends FileInputConfig {
  setWorkerClass(classOf[HDFSTxtInputWorker].getName)
}
