package com.zxin.spark.pipeline.config

import java.text.SimpleDateFormat
import java.util.Date

import com.zxin.spark.pipeline.beans.BusinessConfig
import org.apache.commons.lang3.time.DateFormatUtils

object BusConfig {
  private var baseWorkdir: String = _
  private val eventDate: String = DateFormatUtils.format(new Date(), "yyyyMMdd")
  private var busConfig: BusinessConfig = _

  def apply: BusConfig = new BusConfig()
}

class BusConfig extends BaseConfigLoader(Options.parse) {

  /**
   * 解析任务文件
   *
   * @param configFile 任务文件
   * @return 参数封装的BusConfigBean
   */
  def parseConfigFile(configFile: String): BusinessConfig = {
    val config = AppConfigLoader.loadAppConfig(configFile)
    config.eventDate = BusConfig.eventDate
    BusConfig.busConfig = config
    BusConfig.baseWorkdir = config.persistDir
    logger.debug(s"pipeline config loaded from $configFile")
    config
  }

  /**
   * 获取工作目录
   *
   * @return 工作目录
   */
  def getBaseWorkdir(): String = {
    BusConfig.baseWorkdir
  }

  def getEventDate8(): String = {
    BusConfig.eventDate
  }

  def getEventDateByDate(): Date = {
    new SimpleDateFormat("yyyyMMdd").parse(BusConfig.eventDate)
  }

  def getEventDate10(): String = {
    val date = getEventDateByDate()
    val str = new SimpleDateFormat("yyyy-MM-dd").format(date)
    str
  }

  def getConfig(): BusinessConfig = {
    BusConfig.busConfig
  }
}
