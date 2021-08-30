package com.zxin.spark.pipeline.stages

import java.util
import java.util.concurrent.LinkedBlockingQueue

import com.zxin.spark.pipeline.beans.transform.BaseTransformConfig
import com.zxin.spark.pipeline.beans.{BaseConfig, BusinessConfig}
import com.zxin.spark.pipeline.constants.SysConstants
import com.zxin.spark.pipeline.stages.Transform.SQLTransformWorker
import com.zxin.spark.pipeline.stages.output.OutputStrategies
import com.zxin.spark.pipeline.uitils.AppUtil
import org.apache.spark.sql.SparkSession
import collection.JavaConverters._

object PipelineTask {

  def runTask(config: BusinessConfig, source: String)(implicit ss: SparkSession): Unit = {
    processStage(config.processes, source)(ss)
    outputAction(config.outputs)(ss)
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
