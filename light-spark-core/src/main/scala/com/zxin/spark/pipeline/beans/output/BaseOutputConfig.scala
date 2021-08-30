package com.zxin.spark.pipeline.beans.output

import com.zxin.spark.pipeline.beans.{BaseConfig, NodeTypes}
import org.apache.commons.lang3.StringUtils

class BaseOutputConfig extends BaseConfig {
  tag = NodeTypes.outputs.toString

  override def nameCheck(): Unit = {
    super.nameCheck()
    require(StringUtils.isNotBlank(this.`type`), s"In node '${this.tag}', 'type' is required in item '${this.name}'!")
  }
}
