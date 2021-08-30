package com.zxin.spark.pipeline.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.zxin.spark.pipeline.uitils.{Logging, ResourceUtil}
import org.yaml.snakeyaml.Yaml

import scala.reflect.ClassTag

trait ConfigLoader extends Logging {
  /**
   * 根据 Bean 解析yaml
   *
   * @param file yaml
   * @param cls  Bean
   * @tparam T Bean类型
   * @return yaml 解析后的 Bean
   */
  protected def load[T: ClassTag](file: String, cls: Class[T]): T = {
    val url = ResourceUtil.apply.get(file)
    logger.info(s"located config file path for $file: $url")
    val loader = new Yaml()
    loader.loadAs(url.openStream(), cls)
  }

  /**
   * yaml 文件转为 string
   *
   * @param file yaml file
   * @return
   */
  protected def loadYaml2String(file: String): String = {
    val mapper = new ObjectMapper(new YAMLFactory())
    val stream = this.getClass.getClassLoader.getResourceAsStream(file)
    val str = mapper.readValue(stream, classOf[Object])
    val objectMapper = new ObjectMapper()
    objectMapper.writeValueAsString(str)
  }
}
