package com.zxin.spark.pipeline.stages.output

import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.beans.output.KafkaOutputConfig
import org.apache.spark.sql.SparkSession

class KafkaJsonOutputWorker extends KafkaOutputWorker {
  override def process(config: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = config.asInstanceOf[KafkaOutputConfig]
    ss.table(item.getSrcName).toJSON.rdd.foreachPartition { case iter =>
      sendData(iter, item.brokers, item.topic)
    }
  }
}
