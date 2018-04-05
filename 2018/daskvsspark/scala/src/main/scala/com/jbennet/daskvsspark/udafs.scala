package com.jbennet.daskvsspark.udafs

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


/**
  * Aggregate Counter. Counts values and returns a Map with "value" -> count.
  */
class AggregateCounter extends UserDefinedAggregateFunction {

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(
      StructField("value", StringType) :: Nil
    )

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType =
    StructType(
      StructField("counter", MapType(StringType, IntegerType)) :: Nil
    )

  // This is the output type of your aggregation function.
  override def dataType: DataType = MapType(StringType, IntegerType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var map = buffer.getAs[Map[String, Integer]](0)
    var value = input.getAs[String](0)
    buffer(0) = addValue(map, value)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var m1 = buffer1.getAs[Map[String, Integer]](0)
    var m2 = buffer2.getAs[Map[String, Integer]](0)
    buffer1(0) = mergeMap(m1, m2)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getMap(0)
  }

  /** Add value to map.
    *
    * @param map map
    * @param value value
    * @return map
    */
  def addValue(map: Map[String, Integer], value: String): Map[String, Integer] = (map, value) match {
    case (null, null) => Map()
    case (null, v) => Map(v -> 1)
    case (m, null) => m
    case _ =>
      val zero: Integer = 0
      if (map.contains(value)) map + (value -> (map.getOrElse(value, zero) + 1))
      else map + (value -> 1)
  }

  /** Add two maps into one.
    *
    * @param a first map to merge
    * @param b second map to merge
    * @return merged map
    */
  def mergeMap(a: Map[String, Integer], b: Map[String, Integer]): Map[String, Integer] = (a, b) match {
    case (null, null) => null
    case (null, y) => y
    case (x, null) => x
    case _ =>
      val zero: Integer = 0
      (a.keySet ++ b.keySet).map(
        k => {
          val v1: Integer = a.getOrElse(k, 0)
          val v2: Integer = b.getOrElse(k, 0)
          k -> (v1 + v2:Integer)
        }
      ).toMap
  }
}
