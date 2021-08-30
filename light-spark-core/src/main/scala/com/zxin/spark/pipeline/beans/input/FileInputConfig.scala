package com.zxin.spark.pipeline.beans.input

import com.zxin.spark.pipeline.constants.SysConstants
import org.apache.commons.lang3.StringUtils

import scala.beans.BeanProperty

class FileInputConfig extends BaseInputConfig {

  @BeanProperty
  var columns: String = _
  @BeanProperty
  var path: String = _
  @BeanProperty
  var fs: String = _

  override def setFieldDefault(): Unit = {
    Option(this).filter(obj => StringUtils.isBlank(obj.fs))
      .foreach(obj => {
        obj.fs = SysConstants.apply.HDFS_DATA_DEFAULT_FIELD_SEPARATOR
        logger.info(s"in '$tag', item '$name' field 'fs' is undefind, set default '\\u0001'.")
      })
  }

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("columns", "path", "fs")
  }
}
