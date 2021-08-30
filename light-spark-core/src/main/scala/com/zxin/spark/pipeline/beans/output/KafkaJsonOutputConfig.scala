package com.zxin.spark.pipeline.beans.output

import com.zxin.spark.pipeline.stages.output.KafkaJsonOutputWorker

class KafkaJsonOutputConfig extends KafkaOutputConfig {

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("srcName", "brokers", "topic")
  }

  setWorkerClass(classOf[KafkaJsonOutputWorker].getName)
}
