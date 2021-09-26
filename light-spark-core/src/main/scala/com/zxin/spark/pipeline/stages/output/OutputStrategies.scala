package com.zxin.spark.pipeline.stages.output

import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.stages.BaseWorker
import org.apache.spark.sql.SparkSession

object OutputStrategies {

  val outputWorkers: Map[String, BaseWorker] = Map(
    "hdfsfile" -> new HdfsOutputWorker,
    "hive" -> new HiveOutputWorker,
    "jdbc" -> new JdbcOutputWorker)

  def outputStage(items: List[_ <: BaseConfig])(implicit ss: SparkSession): Unit = {
    items.foreach(output => {
      val worker = outputWorkers.get(output.getType)
      if (worker.isDefined) {
        worker.get.process(output)(ss)
      }
    })
  }

}
