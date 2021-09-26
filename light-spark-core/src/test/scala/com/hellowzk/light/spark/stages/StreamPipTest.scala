package com.zxin.spark.pipeline.stages
import com.zxin.spark.pipeline.beans.transform.BaseTransformConfig
import org.junit.{Assert, Test}

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

class StreamPipTest {

  @Test
  def test_find_pipeline_lineage(): Unit = {
    val lineage = new ListBuffer[(String, String, BaseTransformConfig)]()
    lineage.append((s"sourceTable", "a", new BaseTransformConfig("select a from ${sourceTable}")))
    lineage.append((s"sourceTable", "b", new BaseTransformConfig("select b from ${sourceTable}")))
    lineage.append((s"sourceTable", "c", new BaseTransformConfig("select c from ${sourceTable}")))
    lineage.append((s"a", "e", new BaseTransformConfig("select e from ${a} join ${b}")))
    lineage.append((s"b", "e", new BaseTransformConfig("select e from ${a} join ${b}")))
    lineage.append((s"b", "d", new BaseTransformConfig("select d from ${b} join ${c}")))
    lineage.append((s"c", "d", new BaseTransformConfig("select d from ${b} join ${c}")))
    lineage.append((s"e", "final", new BaseTransformConfig("select final from ${e} join ${d}")))
    lineage.append((s"d", "final", new BaseTransformConfig("select final from ${e} join ${d}")))

    val lineageRelations = Pipeline.bfs(lineage.toList, "sourceTable").asScala.keys.mkString(",")
    Assert.assertEquals("a,b,c,e,d,final", lineageRelations)
  }
}
