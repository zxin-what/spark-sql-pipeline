package com.zxin.spark.pipeline.uitils

import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object KafkaUtils {
  /**
   * 获取 kafka producer
   *
   * @param brokerlist
   * @return
   */
  def getKafkaProducer(brokerlist: String): KafkaProducer[String, String] = {
    val properties = new Properties
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist)
    properties.put("acks", "all")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer") //key 序列号方式
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer") //value 序列号方式
    val producer = new KafkaProducer[String, String](properties)
    producer
  }

  /**
   * 保存数据到 kafka，发送完成后要关闭连接
   *
   * @param producer
   * @param topic
   * @param line
   * @param key
   */
  def sendMessage(producer: KafkaProducer[String, String], topic: String, line: String, key: String = null): Unit = {
    val record = if (StringUtils.isBlank(key))
      new ProducerRecord[String, String](topic, line)
    else
      new ProducerRecord[String, String](topic, key, line)
    producer.send(record)
  }
}
