package com.zxin.spark.pipeline

import com.zxin.spark.pipeline.beans.input._
import com.zxin.spark.pipeline.beans.output._
import com.zxin.spark.pipeline.beans.transform.{BaseTransformConfig, CustomTransformConfig, SQLTransformConfig}
import com.zxin.spark.pipeline.beans.{InputTypes, OutputTypes, ProcessTypes}


object ConfigMapping {
  // input 支持的类型
  val inputBeans = Map(InputTypes.classpathFile.toString -> classOf[ClasspathFileInputConfig],
    InputTypes.classpathFile.toString -> classOf[ClasspathFileInputConfig],
    InputTypes.hdfscsv.toString -> classOf[HDFSCsvInputConfig],
    InputTypes.hdfsfile.toString -> classOf[TxtInputConfig],
    InputTypes.hive.toString -> classOf[HiveInputConfig],
    InputTypes.jdbc.toString -> classOf[JDBCInputConfig],
    InputTypes.kafka.toString -> classOf[KafkaInputConfig],
    InputTypes.customClasspath.toString -> classOf[CustomClasspathInputConfig],
    InputTypes.customHdfs.toString -> classOf[CustomHDFSInputConfig]
  )

  // process 支持的类型
  val processBeans = Map(ProcessTypes.sql.toString -> classOf[SQLTransformConfig],
    ProcessTypes.clazz.toString -> classOf[CustomTransformConfig]
  )
  // output
  val outputBeans = Map(OutputTypes.hive.toString -> classOf[HiveOutputConfig],
    OutputTypes.jdbc.toString -> classOf[JDBCOutputConfig],
    OutputTypes.kafkaField.toString -> classOf[KafkaFieldOutputConfig],
    OutputTypes.kafkaJson.toString -> classOf[KafkaJsonOutputConfig],
    OutputTypes.hdfsfile.toString -> classOf[HDFSOutputConfig]
  )

  def getInputConfigClass(typeName: String): Class[_ <: BaseInputConfig] = {
    val config = inputBeans.getOrElse(typeName, null)
    config
  }

  def getProcessConfigClass(typeName: String): Class[_ <: BaseTransformConfig] = {
    val config = processBeans.getOrElse(typeName, null)
    config
  }

  def getOutputConfigClass(typeName: String): Class[_ <: BaseOutputConfig] = {
    val config = outputBeans.getOrElse(typeName, null)
    config
  }
}
