package com.zxin.spark.pipeline.stages.output

import com.zxin.spark.pipeline.beans.output.HDFSOutputConfig
import com.zxin.spark.pipeline.beans.{BaseConfig, HDFSOutputFormats}
import com.zxin.spark.pipeline.stages.BaseWorker
import com.zxin.spark.pipeline.uitils.HDFSUtils
import org.apache.spark.sql.SparkSession

object HdfsOutputWorker {
  def apply: HdfsOutputWorker = new HdfsOutputWorker()
}

class HdfsOutputWorker extends BaseWorker {
  override def process(config: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = config.asInstanceOf[HDFSOutputConfig]
    var df = ss.table(item.srcName)
    Option(item.partitions).filter(part => part > 0).foreach(part => df = df.repartition(part))
    logger.info(s"try to save to ${item.path}.")
    item.format match {
      case f if f == HDFSOutputFormats.txt.toString => HDFSUtils.apply.saveAsTXT(df, item.path, item.fs)(ss.sparkContext)
      case f if f == HDFSOutputFormats.lzo.toString => HDFSUtils.apply.saveAsLZO(df, item.path, item.fs)(ss.sparkContext)
      case f if f == HDFSOutputFormats.csv.toString => HDFSUtils.apply.saveAsCSV(df, item.path)(ss.sparkContext)
      case f if f == HDFSOutputFormats.json.toString => HDFSUtils.apply.saveAsJSON(df, item.path)(ss.sparkContext)
      case f if f == HDFSOutputFormats.parquet.toString => HDFSUtils.apply.saveAsParquet(df, item.path)(ss.sparkContext)
      case _ => throw new Exception(s"in outputs, unsupport format '${item.format}' for output '${item.srcName}'.")
    }
  }
}
