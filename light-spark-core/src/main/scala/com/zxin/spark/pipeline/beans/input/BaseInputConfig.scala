package com.zxin.spark.pipeline.beans.input

import com.zxin.spark.pipeline.beans.{BaseConfig, NodeTypes}
import org.apache.commons.lang3.StringUtils

import scala.beans.BeanProperty

class BaseInputConfig extends BaseConfig {
  tag = NodeTypes.inputs.toString

  @BeanProperty
  val nullable: Boolean = true

  override def nameCheck(): Unit = {
    require(StringUtils.isNotBlank(this.`type`), s"In node '${this.tag}', 'type' is required in item '${this.`type`}'!")
  }

  override def getDefinedTables(): List[String] = {
    List(name)
  }
}
