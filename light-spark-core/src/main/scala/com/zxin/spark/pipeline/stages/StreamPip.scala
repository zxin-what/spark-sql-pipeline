package com.zxin.spark.pipeline.stages

import java.util.Collections
import com.zxin.spark.pipeline.beans.input.{BaseInputConfig, KafkaInputConfig}
import com.zxin.spark.pipeline.beans.{BaseConfig, BusinessConfig}
import com.zxin.spark.pipeline.function.BaseUDF
import com.zxin.spark.pipeline.stages.custom.CustomBaseInput
import com.zxin.spark.pipeline.uitils.{Logging, ReflectUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

import collection.JavaConverters._

object StreamPip extends Logging {

  /**
   * 处理加工逻辑
   *
   * @param config BusConfigBean
   * @param ss     SparkSession
   */
  def startPip(config: BusinessConfig)(implicit ss: SparkSession, ssc: StreamingContext): Unit = {
    logger.info(s"pipline ${config.configFile} ${config.eventDate} start.")
    // 加载 udf
    Option(config.udf).getOrElse(Collections.emptyList()).forEach(udf => ReflectUtils.apply.getInstance[BaseUDF](udf).setup())
    // 加载输入数据，注册成表
    val kafkaInput = config.inputs.asScala.head.asInstanceOf[KafkaInputConfig]
    val dstream = getDstream(kafkaInput)
    dstream.map(record => record.value()).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        // 处理输入数据
        ReflectUtils.apply.getInstance[CustomBaseInput](kafkaInput.getClazz).process(rdd, kafkaInput.sourceTableName)
        // 编排处理过程
        PipelineTask.runTask(config, kafkaInput.sourceTableName)
      }
    })

    if (!config.isDebug) {
      dstream.foreachRDD { rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def getDstream(item: BaseConfig)(implicit ss: SparkSession, ssc: StreamingContext): DStream[ConsumerRecord[String, String]] = {
    val bean = item.asInstanceOf[BaseInputConfig]
    ReflectUtils.apply.getInstance[StreamBaseInputWorker](bean.workerClass).initDS(bean)
  }

}
