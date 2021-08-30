package com.zxin.spark.pipeline.stages.input

import com.zxin.spark.pipeline.beans.BaseConfig
import com.zxin.spark.pipeline.beans.input.FileInputConfig
import com.zxin.spark.pipeline.stages.BaseWorker
import com.zxin.spark.pipeline.uitils.{AppUtil, HDFSUtils}
import org.apache.spark.sql.SparkSession

object ClasspathFileInputWorker {
  def apply(): ClasspathFileInputWorker = new ClasspathFileInputWorker()
}

class ClasspathFileInputWorker extends BaseWorker {
  /**
   * 加载数据
   *
   * @param bean InputItemBean
   * @param ss   SparkSession
   */
  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = bean.asInstanceOf[FileInputConfig]
    val data = HDFSUtils.apply.loadClasspathFile(item.path, item.fs, item.nullable)(ss.sparkContext)
    AppUtil.rddToTable(data, item.fs, item.columns, item.name)
    afterProcess(item)
  }
}
