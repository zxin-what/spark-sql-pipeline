package com.zxin.spark.pipeline.beans.input

import com.zxin.spark.pipeline.stages.input.ClasspathFileInputWorker

class ClasspathFileInputConfig extends FileInputConfig {
  setWorkerClass(classOf[ClasspathFileInputWorker].getName)
}
