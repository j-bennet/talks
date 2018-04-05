package com.jbennet.daskvsspark

import com.holdenkarau.spark.testing._
import com.jbennet.daskvsspark.udafs.AggregateCounter
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite


/**
  * Tests for AggregateSet
  */
class AggregateCounterTest extends FunSuite with DataFrameSuiteBase {

  private val schema = StructType(Array(
    StructField("url", StringType),
    StructField("referrer", StringType)
  ))

  private val aggcount = new AggregateCounter

  test("different keys should combine") {
    val data = Array(
      Row("url1", "ref2"),
      Row("url1", "ref1")
    )
    val df = sqlContext.createDataFrame(sc.parallelize(data), schema)
    val rows = df.groupBy("url")
      .agg(aggcount(col("referrer")))
      .collect()
    val agg1: Map[String, Integer] = rows(0)(1).asInstanceOf[Map[String, Integer]]
    assert(agg1.size == 2)
    assert(agg1 == Map("ref1" -> (1:Integer), "ref2" -> (1:Integer)))
  }

  test("same keys should add") {
    val data = Array(
      Row("url1", "ref1"),
      Row("url1", "ref1")
    )
    val df = sqlContext.createDataFrame(sc.parallelize(data), schema)
    val rows = df.groupBy("url")
      .agg(aggcount(col("referrer")))
      .collect()
    val agg1: Map[String, Integer] = rows(0)(1).asInstanceOf[Map[String, Integer]]
    assert(agg1.size == 1)
    assert(agg1 == Map("ref1" -> (2:Integer)))
  }

  test("null keys do not count") {
    val data = Array(
      Row("url1", null),
      Row("url1", "ref1")
    )
    val df = sqlContext.createDataFrame(sc.parallelize(data), schema)
    val rows = df.groupBy("url")
      .agg(aggcount(col("referrer")))
      .collect()
    val agg1: Map[String, Integer] = rows(0)(1).asInstanceOf[Map[String, Integer]]
    assert(agg1.size == 1)
    assert(agg1 == Map("ref1" -> (1:Integer)))
  }
}
