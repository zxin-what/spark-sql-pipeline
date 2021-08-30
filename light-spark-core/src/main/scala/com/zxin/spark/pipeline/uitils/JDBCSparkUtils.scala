package com.zxin.spark.pipeline.uitils

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import com.zxin.spark.pipeline.beans.BaseConfig

import scala.collection.JavaConversions._


object JDBCSparkUtils {
  /**
   * spark options 处理只支持 string 值
   */
  def filterValues(item: BaseConfig): util.HashMap[String, String] = {
    val json = JSON.toJSONString(item, new Array[SerializeFilter](0))
    val baseMap = JSON.parseObject(json, classOf[java.util.HashMap[String, Object]])
    val res = new util.HashMap[String, String]()
    baseMap.foreach { case (key, value) =>
      value match {
        case str: String => res.put(key, str)
        case _ =>
      }
    }
    res
  }
}
