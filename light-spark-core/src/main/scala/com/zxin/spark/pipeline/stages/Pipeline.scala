package com.zxin.spark.pipeline.stages

import java.util
import java.util.concurrent.LinkedBlockingQueue

import com.zxin.spark.pipeline.beans.{BaseConfig, BusinessConfig}
import com.zxin.spark.pipeline.beans.transform.BaseTransformConfig
import com.zxin.spark.pipeline.constants.SysConstants
import com.zxin.spark.pipeline.function.BaseUDF
import com.zxin.spark.pipeline.stages.transform.SQLTransformWorker
import com.zxin.spark.pipeline.stages.output.OutputStrategies
import com.zxin.spark.pipeline.uitils.{AppUtil, ReflectUtils, SparkUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import collection.JavaConverters._

object Pipeline {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getCanonicalName.stripSuffix("$"))

  def runTask(config: BusinessConfig)(implicit ss: SparkSession): Unit = {
    logger.info(s"pipeline ${config.configFile} ${config.eventDate} start.")
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
    inputAction(config.inputs, StageType.inputs.toString)
    processStage(config.processes, config.inputs.head.name)
    outputAction(config.outputs)
    SparkUtil.uncacheData()
    logger.info(s"pipline ${config.configFile} ${config.eventDate} finished.")
  }

  def inputAction(items: java.util.List[_ <: BaseConfig], stageName: String)(implicit ss: SparkSession): Unit = {
    Option(items).filter(!_.isEmpty).foreach(lst => {
      for (i <- lst.indices) {
        val item = lst(i)
        logger.info(s"start $stageName, step${i + 1}, item '${item.name}'.")
        ReflectUtils.apply.getInstance[BaseWorker](item.workerClass).process(item)
      }
    })
  }

  /**
   * pipeline 处理
   *
   * @param items     items
   * @param stageName stageName
   * @param ss        ss
   */
  def processStage(items: java.util.List[_ <: BaseTransformConfig], stageName: String)(implicit ss: SparkSession): Unit = {
    val stepOrder = bfs(items.asScala.flatMap(item => AppUtil.regexFind(SysConstants.apply.VARIABLE_REGEX, item, rule = true)).toList, stageName)
    stepOrder.asScala.foreach(step => {
      val worker = new SQLTransformWorker
      worker.process(step._2)(ss)
    })
  }

  def outputAction(items: java.util.List[_ <: BaseConfig])(implicit ss: SparkSession): Unit = {
    OutputStrategies.outputStage(items.asScala.toList)(ss)
  }

  /**
   * 广度优先搜索找出step执行顺序
   *
   * @param steps       steps
   * @param sourceTable sourceTable
   * @return
   */
  def bfs(steps: List[(String, String, BaseConfig)], sourceTable: String): util.LinkedHashMap[String, BaseConfig] = {
    val lineage = new util.LinkedHashMap[String, BaseConfig]()
    val queue = new LinkedBlockingQueue[(String, String, BaseConfig)]()
    var left = steps
    queue.put(("", sourceTable, null))
    while (!queue.isEmpty) {
      val now = queue.poll()
      if (now._3 != null) {
        val sql = now._3.sql
        AppUtil.regexFindWithRule(SysConstants.apply.VARIABLE_REGEX, sql)
          .zip(AppUtil.regexFindWithoutRule(SysConstants.apply.VARIABLE_REGEX, sql))
          .foreach(rule => now._3.setSql(sql.replace(rule._1, rule._2)))
        lineage.put(now._2, now._3)
      }
      val (children, other) = left.partition(_._1.equals(now._2))
      left = other
      children.foreach(child => {
        queue.add(child)
      })
    }
    lineage
  }
}
