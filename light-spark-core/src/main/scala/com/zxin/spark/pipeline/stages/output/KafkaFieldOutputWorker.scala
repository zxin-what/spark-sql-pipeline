package com.zxin.spark.pipeline.stages.output

import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.beans.output.KafkaFieldOutputConfig
import org.apache.spark.sql.SparkSession

class KafkaFieldOutputWorker extends KafkaOutputWorker {
  override def process(config: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = config.asInstanceOf[KafkaFieldOutputConfig]
    ss.table(item.srcName).rdd.map(a => a.toSeq.mkString(item.fs)).foreachPartition { case iter =>
      sendData(iter, item.brokers, item.topic)
    }
  }
}
