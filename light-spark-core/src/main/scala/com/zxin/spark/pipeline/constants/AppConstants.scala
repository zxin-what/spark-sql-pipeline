package com.zxin.spark.pipeline.constants

import java.util.Date

import com.zxin.spark.pipeline.config.BusConfig
import org.apache.commons.lang3.time.DateFormatUtils

object AppConstants {
  var variables = scala.collection.mutable.Map.empty[String, String]

  def apply: AppConstants = new AppConstants()
}

class AppConstants {
  val EVENT_DATE: String = if (null == BusConfig.apply.getEventDate8()) DateFormatUtils.format(new Date(), "yyyyMMdd") else BusConfig.apply.getEventDate8()
  val EVENT_DATE10: String = if (null == BusConfig.apply.getEventDate10()) DateFormatUtils.format(new Date(), "yyyy-MM-dd") else BusConfig.apply.getEventDate10()
  /*
   * 排除 sonar 运行时添加的变量
   */
  val SONAR_VARIABLE: Set[String] = Set("$jacocoData")
}
