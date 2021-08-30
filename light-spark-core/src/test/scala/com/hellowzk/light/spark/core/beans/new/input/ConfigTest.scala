package com.zxin.spark.pipeline.core.beans.`new`.input

import com.zxin.spark.pipeline.beans.input.JDBCInputConfig
import org.apache.commons.collections.{CollectionUtils, Predicate}
import org.apache.commons.lang3.StringUtils
import org.junit.Test

import scala.collection.JavaConversions._

class ConfigTest {
  @Test
  def testCheck(): Unit = {
    val jdbcInputConfig = new JDBCInputConfig()
    jdbcInputConfig.setName("testinput")
    jdbcInputConfig.setDriver("aa")
    jdbcInputConfig.setUrl("jdbc:xxx")
    jdbcInputConfig.setUser("user-root")
    jdbcInputConfig.setPassword("passwd-root")
    jdbcInputConfig.setDbtable(Map("a" -> "b"))
    jdbcInputConfig.doCheck()
    println("finish")
  }

  @Test
  def isAllBlank(): Unit = {

    val b = Map("a" -> "b")
    val a = StringUtils.isAnyBlank("null", "a", "b", null)

    val c = CollectionUtils.select(b, new Predicate() {
      override def evaluate(o: Any): Boolean = {
        o
        println(o)
        true
      }
    })
    c.foreach(println)
  }
}
