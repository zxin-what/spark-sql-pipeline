package com.zxin.spark.pipeline.stages

import com.zxin.spark.pipeline.beans.{BaseConfig, BusinessConfig}
import com.zxin.spark.pipeline.function.BaseUDF
import com.zxin.spark.pipeline.uitils.{Logging, ReflectUtils, SparkUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

import scala.collection.JavaConversions._

object BatchPip extends Logging {
  /**
   * 开始处理加工逻辑
   *
   * @param config BusConfigBean
   * @param ss     SparkSession
   */
  def startPip(config: BusinessConfig)(implicit ss: SparkSession): Unit = {
    logger.info(s"pipline ${config.configFile} ${config.eventDate} start.")
    // 加载 udaf
    Option(config.udaf).filter(_.nonEmpty).foreach(clazzs =>
      clazzs.foreach { case (udafName, udafCls) =>
        val instans = Class.forName(udafCls).newInstance()
        Option(instans).filter(obj => obj.isInstanceOf[UserDefinedAggregateFunction]).foreach(obj => {
          ss.udf.register(udafName, obj.asInstanceOf[UserDefinedAggregateFunction])
          logger.info(s"registerd UDAF: '$udafCls' => '$udafName'.")
        })
      })
    Option(config.udf).filter(_.nonEmpty).foreach(clazzs =>
      clazzs.foreach { case udf =>
        ReflectUtils.apply.getInstance[BaseUDF](udf).setup()
      })
    // 加载输入数据，注册成表
    logger.info("----------------------start inputs----------------------")
    processStage(config.inputs, StageType.inputs.toString)
    logger.info("----------------------start processes----------------------")
    PipelineTask.runTask(config, config.inputs.head.name)(ss)
    SparkUtil.uncacheData()
    logger.info(s"pipline ${config.configFile} ${config.eventDate} finished.")
  }

  /**
   * pipeline 处理
   *
   * @param items     items
   * @param stageName stageName
   * @param ss        ss
   */
  def processStage(items: java.util.List[_ <: BaseConfig], stageName: String)(implicit ss: SparkSession): Unit = {
    Option(items).filter(!_.isEmpty).foreach(lst => {
      for (i <- lst.indices) {
        val item = lst(i)
        logger.info(s"start $stageName, step${i + 1}, item '${item.name}'.")
        ReflectUtils.apply.getInstance[BaseWorker](item.workerClass).process(item)
      }
    })
  }
}
