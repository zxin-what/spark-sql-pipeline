package com.zxin.spark.pipeline.beans

import java.util

import com.zxin.spark.pipeline.beans.input.BaseInputConfig
import com.zxin.spark.pipeline.beans.output.BaseOutputConfig
import com.zxin.spark.pipeline.beans.transform.BaseTransformConfig

import scala.beans.BeanProperty
class BusinessConfig extends BaseConfig {
  var configFile: String = _
  var eventDate: String = _
  var isStreaming: Boolean = false
  // 配置的所有 store 只有 isDebug = true 时，生效
  var isDebug: Boolean = false

  @BeanProperty
  var streamBatchSeconds: java.lang.Long = 0l

  @BeanProperty
  var constansCls: String = _

  @BeanProperty
  var constansMap: java.util.Map[String, String] = new util.HashMap[String, String]()

  @BeanProperty
  var udf: java.util.List[String] = _

  @BeanProperty
  var udaf: java.util.Map[String, String] = _

  // hdfs hive
  @BeanProperty
  var persistType: String = _

  @BeanProperty
  var persistDir: String = _

  @BeanProperty
  var enableShow: Boolean = true

  @BeanProperty
  var persistHiveDb: String = _

  @BeanProperty
  var inputs: java.util.List[BaseInputConfig] = _

  @BeanProperty
  var processes: java.util.List[BaseTransformConfig] = _

  @BeanProperty
  var outputs: java.util.List[BaseOutputConfig] = _

  @BeanProperty
  var hiveEnabled: Boolean = true

  @BeanProperty
  var envs: EnvConfig = new EnvConfig()

  /**
   * 检查必填项，扩展支持的组件时需要实现
   */
  override def doCheck(): Unit = {
    validateNoneIsBlank("inputs")
  }

}
