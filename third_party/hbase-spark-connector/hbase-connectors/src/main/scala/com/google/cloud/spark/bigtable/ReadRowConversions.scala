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

package com.google.cloud.spark.bigtable

import com.google.cloud.bigtable.data.v2.models.{RowCell, Row => BigtableRow}
import com.google.cloud.spark.bigtable.catalog.ParsingResult
import com.google.cloud.spark.bigtable.datasources.{BigtableTableCatalog, Field, ReadRowUtil, Utils}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row => SparkRow}
import com.google.re2j.Pattern

object ReadRowConversions extends Serializable {

  private def buildRowForRegex(
      cqFields: Seq[Field],
      bigtableRow: BigtableRow
  ): Seq[(Field, Any)] = {
    cqFields.map { x =>
      val pattern: Pattern = Pattern.compile(x.btColName)
      val allCells: List[RowCell] = ReadRowUtil.getAllCells(bigtableRow, x.btColFamily)
      val cqAllFields = allCells
        .filter(cell => pattern.matcher(cell.getQualifier.toStringUtf8).find())
        .map { cell =>
          val qualifier = cell.getQualifier.toStringUtf8
          val cqField = Field(
            x.sparkColName,
            x.btColFamily,
            qualifier,
            x.simpleType,
            x.avroSchema,
            x.len
          )
          extractValue(cqField, bigtableRow)
        }
      val cqValues = cqAllFields.map { case (field, value) => field.btColName -> value }
      (x, cqValues.toMap)
    }
  }

  /** Takes a Bigtable Row and extracts the fields in the rowkey from it.
    *
    * @param bigtableRow  The Bigtable row
    * @param keyFields    The fields in the row key, ordered by their
    *                       order of appearance in the catalog
    * @param catalog      The catalog for converting from Spark SQL
    *                       DataFrame to a Bigtable table
    * @return             A map from fields in the rowkey to the their value
    */
  def parseRowKey(
      bigtableRow: BigtableRow,
      keyFields: Seq[Field],
      catalog: BigtableTableCatalog
  ): Map[Field, Any] = {
    val row: Array[Byte] = bigtableRow.getKey.toByteArray
    keyFields
      .foldLeft((0, Seq[(Field, Any)]()))((state, field) => {
        val idx = state._1
        val parsed = state._2
        field.fromByteArray(row, idx) match {
          case ParsingResult(value, consumedBytes) =>
            (idx + consumedBytes + 1, parsed ++ Seq((field, value)))
        }
      })
      ._2
      .toMap
  }

  /** Converts a Bigtable Row to a Spark SQL Row.
    *
    * @param fields       The fields required by Spark
    * @param bigtableRow  The Bigtable row
    * @param catalog      The catalog for converting from Spark SQL
    *                       DataFrame to a Bigtable table
    * @return             A Spark SQL row containing all of the required columns
    */
  def buildRow(
      fields: Seq[Field],
      bigtableRow: BigtableRow,
      catalog: BigtableTableCatalog
  ): SparkRow = {
    val keySeq = parseRowKey(bigtableRow, catalog.getRowKeyColumns, catalog)

    val valueSeq = fields
      .filter(!_.isRowKey)
      .map(field => extractValue(field, bigtableRow))
      .toMap
    val cqFields = catalog.sMap.cqMap.values.toSeq
    val cqValueSeq = buildRowForRegex(cqFields, bigtableRow).toMap
    val unionedRow = keySeq ++ valueSeq ++ cqValueSeq
    SparkRow.fromSeq((fields ++ cqFields).map(unionedRow.get(_).orNull))
  }

  private def extractValue(field: Field, bigtableRow: BigtableRow): (Field, Any) = {
    val allCells: List[RowCell] =
      ReadRowUtil.getAllCells(bigtableRow, field.btColFamily, field.btColName)
    if (allCells.isEmpty) {
      (field, null)
    } else {
      val latestCellValue = allCells.head.getValue.toByteArray

      field.fromByteArray(latestCellValue, 0) match {
        case ParsingResult(value, consumedBytes) =>
          if (consumedBytes < latestCellValue.length) {
            throw new IllegalArgumentException(
              "The byte array in Bigtable cell [" + latestCellValue.mkString(
                ", "
              ) +
                "] has length " + latestCellValue.length + ", while column " + field +
                " requires length " + consumedBytes + "."
            )
          }

          (field, value)
      }
    }
  }
}
