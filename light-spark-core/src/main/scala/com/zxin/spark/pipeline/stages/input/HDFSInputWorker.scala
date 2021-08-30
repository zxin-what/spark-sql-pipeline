package com.zxin.spark.pipeline.stages.input

import com.zxin.spark.pipeline.stages.BaseWorker
import com.zxin.spark.pipeline.uitils.HDFSUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait HDFSInputWorker extends BaseWorker {

  protected def loadAsText(path: String, nullable: Boolean)(implicit sc: SparkContext): RDD[String] = {
    if (nullable && !HDFSUtils.apply.exists(path)) {
      sc.emptyRDD[String]
    } else {
      sc.textFile(path)
    }
  }
}
