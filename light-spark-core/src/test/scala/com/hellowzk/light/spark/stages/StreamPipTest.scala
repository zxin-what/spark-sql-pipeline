package com.zxin.spark.pipeline.stages
import java.util

import com.zxin.spark.pipeline.beans.transform.BaseTransformConfig
import org.junit.Test

import scala.collection.mutable.ListBuffer

class StreamPipTest {

  @Test
  def test_find_pipeline_lineage(): Unit = {
    val lineage = new ListBuffer[(String, String, BaseTransformConfig)]()
    lineage.append((s"sourceTable", "a", new BaseTransformConfig()))
    lineage.append((s"sourceTable", "b", new BaseTransformConfig))
    lineage.append((s"sourceTable", "c", new BaseTransformConfig))
    lineage.append((s"a", "e", new BaseTransformConfig))
    lineage.append((s"b", "e", new BaseTransformConfig))
    lineage.append((s"b", "d", new BaseTransformConfig))
    lineage.append((s"c", "d", new BaseTransformConfig))
    lineage.append((s"e", "final", new BaseTransformConfig))
    lineage.append((s"d", "final", new BaseTransformConfig))

    val lineageRelations = PipelineTask.bfs(lineage.toList, "sourceTable")
    println(lineageRelations)
  }
}
