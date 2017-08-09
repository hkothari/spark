/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.RelationStatistics
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.sql.types._

case class TestRow(str: String, dbl: Double, id: Long, allTrue: Boolean, trueAndFalse: Boolean)
case class ComplexRow(simpleValue: Long, complexValue: TestRow)

/**
 * @author hkothari
 */
class ParquetColumnStatsSuite extends ParquetTest with SQLTestUtils with SharedSQLContext {

  import testImplicits._

  test("generated stats not present without schema merging") {
    withSQLConf(
      (SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key, "false")
    ) {
      val data = Seq(
        TestRow("sadfasdf", 213.231, 1, true, false),
        TestRow("dsaf", 21.2, 2, true, false),
        TestRow("d", 10, 3, true, true)
      )
      withParquetDataFrame(data)(df => {
        val stats = df.logicalPlan.asInstanceOf[LogicalRelation].stats
        assert(stats.rowCount.isEmpty)
      })
    }
  }

  test("generated stats are legit") {
    withSQLConf(
      (SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key, "true")
    ) {
      val data = Seq(
        TestRow("sadfasdf", 213.231, 1, true, false),
        TestRow("dsaf", 21.2, 2, true, false),
        TestRow("d", 10, 3, true, true),
        TestRow("sadfasdf", 213.231, 1, true, false),
        TestRow(null, 21.2, 2, true, false),
        TestRow(null, 10, 3, true, false)
      )
      withParquetDataFrame(data)(df => {
        val stats = df.logicalPlan.asInstanceOf[LogicalRelation]
          .relation.asInstanceOf[HadoopFsRelation].dataStatistics.get
        assert(stats.rowCount.get === 6)
        assert(stats.colStats("dbl") ===
          ColumnStat(6, Some(10.0), Some(213.231), 0,
            DoubleType.defaultSize, DoubleType.defaultSize))
        assert(stats.colStats("id") ===
          ColumnStat(3, Some(1), Some(3), 0,
            LongType.defaultSize, LongType.defaultSize))
        assert(stats.colStats("allTrue") ===
          ColumnStat(1, Some(true), Some(true), 0,
            BooleanType.defaultSize, BooleanType.defaultSize))
        assert(stats.colStats("trueAndFalse") ===
          ColumnStat(2, Some(false), Some(true), 0,
            BooleanType.defaultSize, BooleanType.defaultSize))
        assert(stats.colStats("str") ===
          ColumnStat(5, None, None, 2,
            StringType.defaultSize, StringType.defaultSize))
      })
    }
  }

  test("testing dot/space in column name") {
    withSQLConf(
      (SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key, "true")
    ) {
      val data = Seq(
        (5, 1),
        (4, 3)
      ).toDF("dot.column", "columnNames")
      withParquetDataFrame(data)(df => {
        val stats = df.logicalPlan.asInstanceOf[LogicalRelation]
          .relation.asInstanceOf[HadoopFsRelation].dataStatistics.get
        assert(stats.rowCount.get === 2)
        assert(stats.colStats.keys === df.columns.toSet)
      })
    }
  }

  test("ensure nested columns are ignored") {
    // It looks like nested columns are not supported
    // by ColumnStats (see ColumnStat.supportsType)
    // and because of the AttributeMap setup, it seems
    // like it would be hard to support columns with dots in
    // name and nested columns
    // so we'll ignore them for now.

    withSQLConf(
      (SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key, "true")
    ) {
      val data = Seq(
        ComplexRow(4, TestRow("sadfasdf", 213.231, 1, true, false)),
        ComplexRow(3, TestRow("dsaf", 21.2, 2, true, false)),
        ComplexRow(5, TestRow("d", 10, 3, true, true))
      )
      withParquetDataFrame(data)(df => {
        val stats = df.logicalPlan.asInstanceOf[LogicalRelation]
          .relation.asInstanceOf[HadoopFsRelation].dataStatistics.get
        assert(stats.rowCount.get === 3)
        assert(stats.colStats.keySet === Set("simpleValue"))
      })
    }
  }

  Seq(IntegerType, LongType, ShortType).foreach(t => {
    val (min: Any, max1: Any, max2: Any, max3: Any) = t match {
      case IntegerType => (1, 24, 50, 5000)
      case LongType => (1L, 24L, 50L, 5000L)
      case ShortType =>
        (1: Short,
          24: Short,
          50: Short,
          5000: Short)
    }

    test(s"reducing $t - range smaller than rows") {
      val schema = StructType(StructField("c1", t) :: Nil)
      val r1 = RelationStatistics(1024, Some(100), Map("c1" -> ColumnStat(
        25, Some(min), Some(max1), 0, t.defaultSize, t.defaultSize
      )))
      val r2 = RelationStatistics(512, Some(50), Map("c1" -> ColumnStat(
        50, Some(min), Some(max2), 1, t.defaultSize, t.defaultSize
      )))
      val merged = ParquetColumnStats.mergeStats(Seq(Some(r1), Some(r2)), schema).get.colStats("c1")
      assert(merged === ColumnStat(
        51, Some(min), Some(max2), 1, t.defaultSize, t.defaultSize
      ))
    }

    test(s"reducing $t - range larger than rows") {
      val schema = StructType(StructField("c1", t) :: Nil)
      val r1 = RelationStatistics(1024, Some(100), Map("c1" -> ColumnStat(
        100, Some(min), Some(max3), 0, t.defaultSize, t.defaultSize
      )))
      val r2 = RelationStatistics(512, Some(50), Map("c1" -> ColumnStat(
        50, Some(min), Some(max3), 1, t.defaultSize, t.defaultSize
      )))
      val merged = ParquetColumnStats.mergeStats(Seq(Some(r1), Some(r2)), schema).get.colStats("c1")
      assert(merged === ColumnStat(
        150, Some(min), Some(max3), 1, t.defaultSize, t.defaultSize
      ))
    }
  })

  Seq(DoubleType, FloatType).foreach(t => {
    val (min1: Any, min2: Any, max1: Any, max2: Any) = t match {
      case DoubleType => (0.01, -0.15, 1.5, 1.0)
      case FloatType => (0.01f, -0.15f, 1.5f, 1.0f)
    }
    test(s"reducing $t") {
      val schema = StructType( StructField("c1", t) :: Nil )
      val r1 = RelationStatistics(1024, Some(100), Map("c1" -> ColumnStat(
        96, Some(min1), Some(max1), 5, t.defaultSize, t.defaultSize
      )))
      val r2 = RelationStatistics(512, Some(50), Map("c1" -> ColumnStat(
        50, Some(min2), Some(max2), 0, t.defaultSize, t.defaultSize
      )))
      val merged = ParquetColumnStats.mergeStats(Seq(Some(r1), Some(r2)), schema).get.colStats("c1")
      assert(merged === ColumnStat(
        146, Some(min2), Some(max1), 5, t.defaultSize, t.defaultSize
      ))
    }
  })

  test("reducing StringType") {
    val schema = StructType( StructField("c1", StringType) :: Nil )
    val r1 = RelationStatistics(10024, Some(100), Map(
      "c1" -> ColumnStat(100, None, None, 0, StringType.defaultSize, StringType.defaultSize)
    ))
    val r2 = RelationStatistics(5000, Some(50), Map(
      "c1" -> ColumnStat(11, None, None, 5, StringType.defaultSize, StringType.defaultSize)
    ))
    val merged = ParquetColumnStats.mergeStats(Seq(Some(r1), Some(r2)), schema).get.colStats("c1")
    assert(merged === ColumnStat(
      111, None, None, 5, StringType.defaultSize, StringType.defaultSize
    ))
  }

  test("reduce BooleanType - all one value") {
    val schema = StructType( StructField("c1", BooleanType) :: Nil )
    val r1 = RelationStatistics(100, Some(100), Map("c1" -> ColumnStat(
      1, Some(true), Some(true), 0, BooleanType.defaultSize, BooleanType.defaultSize
    )))
    val r2 = RelationStatistics(50, Some(50), Map("c1" -> ColumnStat(
      2, Some(true), Some(true), 1, BooleanType.defaultSize, BooleanType.defaultSize
    )))
    val merged = ParquetColumnStats.mergeStats(Seq(Some(r1), Some(r2)), schema).get.colStats("c1")
    assert(merged === ColumnStat(
      2, Some(true), Some(true), 1, BooleanType.defaultSize, BooleanType.defaultSize
    ))
  }

  test("reduce BooleanType - multiple values") {
    val schema = StructType( StructField("c1", BooleanType) :: Nil )
    val r1 = RelationStatistics(100, Some(100), Map("c1" -> ColumnStat(
      2, Some(false), Some(true), 0, BooleanType.defaultSize, BooleanType.defaultSize
    )))
    val r2 = RelationStatistics(50, Some(50), Map("c1" -> ColumnStat(
      2, Some(true), Some(true), 1, BooleanType.defaultSize, BooleanType.defaultSize
    )))
    val merged = ParquetColumnStats.mergeStats(Seq(Some(r1), Some(r2)), schema).get.colStats("c1")
    assert(merged === ColumnStat(
      3, Some(false), Some(true), 1, BooleanType.defaultSize, BooleanType.defaultSize
    ))
  }

  test("test schema merging - new column") {
    val schema = StructType( StructField("c1", BooleanType) :: Nil )
    val r1 = RelationStatistics(100, Some(100), Map("c1" -> ColumnStat(
      2, Some(false), Some(true), 0, BooleanType.defaultSize, BooleanType.defaultSize
    )))
    val r2 = RelationStatistics(50, Some(50), Map())
    val merged = ParquetColumnStats.mergeStats(Seq(Some(r1), Some(r2)), schema).get.colStats("c1")
    assert(merged === ColumnStat(
      3, Some(false), Some(true), 50, BooleanType.defaultSize, BooleanType.defaultSize
    ))
  }
}
