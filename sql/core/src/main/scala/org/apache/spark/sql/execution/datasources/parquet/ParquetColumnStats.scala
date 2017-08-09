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

import scala.collection.JavaConverters._

import org.apache.parquet.column.statistics._
import org.apache.parquet.hadoop.Footer
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData

import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.sources.RelationStatistics
import org.apache.spark.sql.types._

/**
 * @author hkothari
 */
object ParquetColumnStats {

  private def cardinalityFromRange(lower: BigInt,
                                      upper: BigInt,
                                      numRows: BigInt,
                                      numNulls: BigInt): BigInt = {
    val additional = if (numNulls > 0) 2 else 1
    naiveCardinality(numRows, numNulls).min((upper - lower) + additional)
  }

  private def booleanCardinality(min: Boolean, max: Boolean, numNulls: BigInt): BigInt = {
    val nullCardinality = if (numNulls > 0) 1 else 0
    if (min == max) {
      1 + nullCardinality
    } else {
      2 + nullCardinality
    }
  }

  private def naiveCardinality(numRows: BigInt, numNulls: BigInt) = if (numNulls > 0) {
    numRows - numNulls + 1
  } else {
    numRows
  }

  private def statsFromColumn(numRows: Long,
                              columnChunkMetaData: ColumnChunkMetaData,
                              t: DataType): Option[ColumnStat] = {
    columnChunkMetaData.getStatistics match {
      case s: IntStatistics =>
        Some(ColumnStat(cardinalityFromRange(s.getMin, s.getMax, numRows, s.getNumNulls),
          Some(s.getMin), Some(s.getMax), s.getNumNulls,
          IntegerType.defaultSize, IntegerType.defaultSize))
      case s: LongStatistics =>
        Some(ColumnStat(cardinalityFromRange(s.getMin, s.getMax, numRows, s.getNumNulls),
          Some(s.getMin), Some(s.getMax), s.getNumNulls,
          LongType.defaultSize, LongType.defaultSize))
      case s: FloatStatistics =>
        Some(ColumnStat(naiveCardinality(numRows, s.getNumNulls),
          Some(s.getMin), Some(s.getMax),
          s.getNumNulls, FloatType.defaultSize, FloatType.defaultSize))
      case s: DoubleStatistics =>
        Some(ColumnStat(naiveCardinality(numRows, s.getNumNulls),
          Some(s.getMin), Some(s.getMax),
          s.getNumNulls, DoubleType.defaultSize, DoubleType.defaultSize))
      case s: BooleanStatistics =>
        Some(ColumnStat(
          booleanCardinality(s.getMin, s.getMax, s.getNumNulls),
          Some(s.getMin), Some(s.getMax), s.getNumNulls,
          BooleanType.defaultSize, BooleanType.defaultSize))
      case s: BinaryStatistics =>
        Some(ColumnStat(naiveCardinality(numRows, s.getNumNulls),
          None, None, s.getNumNulls, StringType.defaultSize, StringType.defaultSize))
      case _ => None
    }
  }

  def parseStatsFromFooters(footers: Seq[Footer],
                            schema: StructType): Option[RelationStatistics] = {
    mergeStats(footers.map(parseStatsFromFooter(_, schema)), schema)
  }

  def parseStatsFromFooter(footer: Footer, schema: StructType): Option[RelationStatistics] = {
    val stats = footer.getParquetMetadata.getBlocks.asScala.map(b => {
      val attrs = b.getColumns.asScala.flatMap(c => {
        val typ = dataTypeFromName(schema, c.getPath.toDotString)
        typ.fold[Option[(String, ColumnStat)]](None)(t => {
          statsFromColumn(b.getRowCount, c, t).map((c.getPath.toDotString, _))
        })
      }).toMap
      Some(RelationStatistics(b.getTotalByteSize, Some(b.getRowCount), attrs))
    })
    if (stats.nonEmpty) {
      mergeStats(stats, schema)
    } else {
      None
    }
  }

  private def maxType(left: Option[Any], right: Option[Any], t: DataType): Option[Any] = {
    (left, right, t) match {
      case (Some(l), Some(r), IntegerType) =>
        Some(Math.max(l.asInstanceOf[Int], r.asInstanceOf[Int]))
      case (Some(l), Some(r), LongType) =>
        Some(Math.max(l.asInstanceOf[Long], r.asInstanceOf[Long]))
      case (Some(l), Some(r), ShortType) =>
        Some(Math.max(l.asInstanceOf[Short], r.asInstanceOf[Short]))
      case (Some(l), Some(r), FloatType) =>
        Some(Math.max(l.asInstanceOf[Float], r.asInstanceOf[Float]))
      case (Some(l), Some(r), DoubleType) =>
        Some(Math.max(l.asInstanceOf[Double], r.asInstanceOf[Double]))
      case (Some(l), Some(r), BooleanType) =>
        Some(l.asInstanceOf[Boolean] || r.asInstanceOf[Boolean])
      case (Some(l), None, _) =>
        Some(l)
      case (None, Some(r), _) =>
        Some(r)
      case _ => None
    }
  }

  private def minType(left: Option[Any], right: Option[Any], t: DataType): Option[Any] = {
    (left, right, t) match {
      case (Some(l), Some(r), IntegerType) =>
        Some(Math.min(l.asInstanceOf[Int], r.asInstanceOf[Int]))
      case (Some(l), Some(r), LongType) =>
        Some(Math.min(l.asInstanceOf[Long], r.asInstanceOf[Long]))
      case (Some(l), Some(r), ShortType) =>
        Some(Math.min(l.asInstanceOf[Short], r.asInstanceOf[Short]))
      case (Some(l), Some(r), FloatType) =>
        Some(Math.min(l.asInstanceOf[Float], r.asInstanceOf[Float]))
      case (Some(l), Some(r), DoubleType) =>
        Some(Math.min(l.asInstanceOf[Double], r.asInstanceOf[Double]))
      case (Some(l), Some(r), BooleanType) =>
        Some(l.asInstanceOf[Boolean] && r.asInstanceOf[Boolean])
      case (Some(l), None, _) =>
        Some(l)
      case (None, Some(r), _) =>
        Some(r)
      case _ => None
    }
  }

  def mergeColumn(left: ColumnStat, leftRows: BigInt,
                  right: ColumnStat, rightRows: BigInt, t: DataType): ColumnStat = {
    val numRows = leftRows + rightRows
    val newNulls = left.nullCount + right.nullCount
    val newAvgLength = (
      ((BigInt(left.avgLen) * leftRows)
        + (BigInt(right.avgLen) * rightRows)
      ) / (leftRows + rightRows)).toLong
    val newMaxLength = Math.max(left.maxLen, right.maxLen)
    val newMin = minType(left.min, right.min, t)
    val newMax = maxType(left.max, right.max, t)
    val newCardinality = (newMin, newMax, t) match {
      case (Some(min: Int), Some(max: Int), IntegerType | ShortType) =>
        cardinalityFromRange(min, max, numRows, newNulls)
      case (Some(min: Long), Some(max: Long), LongType | IntegerType | ShortType) =>
        cardinalityFromRange(min, max, numRows, newNulls)
      case (Some(min: Short), Some(max: Short), ShortType) =>
        cardinalityFromRange(BigInt(min), BigInt(max), numRows, newNulls)
      case (Some(min: Boolean), Some(max: Boolean), BooleanType) =>
        booleanCardinality(min, max, newNulls)
      case _ =>
        naiveCardinality(numRows, newNulls).min(left.distinctCount + right.distinctCount)
    }
    ColumnStat(newCardinality, newMin, newMax, newNulls, newAvgLength, newMaxLength)
  }

  def dataTypeFromName(schema: StructType, name: String): Option[DataType] =
    schema.find(_.name == name).map(_.dataType)

  def mergeColumnStats(left: Map[String, ColumnStat],
                       leftRows: BigInt,
                       right: Map[String, ColumnStat],
                       rightRows: BigInt,
                       schema: StructType): Map[String, ColumnStat] = {
    val allKeys = left.keySet ++ right.keySet
    allKeys
      .map(k => (k, left.get(k), right.get(k), dataTypeFromName(schema, k)))
      .filter(_._4.isDefined)
      .map({
        case (k, Some(l), Some(r), Some(t)) =>
          (k, mergeColumn(l, leftRows, r, rightRows, t))
        case (k, Some(l), None, Some(t)) =>
          val tempStat = ColumnStat(1, None, None, rightRows, t.defaultSize, t.defaultSize)
          (k, mergeColumn(l, leftRows, tempStat, rightRows, t))
        case (k, None, Some(r), Some(t)) =>
          val tempStat = ColumnStat(1, None, None, rightRows, t.defaultSize, t.defaultSize)
          (k, mergeColumn(tempStat, leftRows, r, rightRows, t))
        case _ => sys.error("This shouldn't be possible..")
      }).toMap
  }

  def mergeStats(stats: Seq[Option[RelationStatistics]],
                 schema: StructType): Option[RelationStatistics] = {
    val flattened = stats.flatten
    if (stats.isEmpty || flattened.length != stats.length) {
      None
    } else {
      Some(flattened.reduce((left, right) => {
        RelationStatistics(
          left.sizeInBytes + left.sizeInBytes,
          Some(left.rowCount.get + right.rowCount.get),
          mergeColumnStats(left.colStats, left.rowCount.get,
            right.colStats, right.rowCount.get, schema)
        )
      }))
    }
  }

}
