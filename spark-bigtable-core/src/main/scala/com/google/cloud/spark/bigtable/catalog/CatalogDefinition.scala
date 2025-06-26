package com.google.cloud.spark.bigtable.catalog

object CatalogDefinition()

case class CatalogDefinition(table: TableDefinition,
                             rowKey: RowKeyDefinition,
                             columns: ColumnsDefinition,
                             regexColumns: Option[RegexColumnsDefinition])

case class TableDefinition(name: String)

type RowKeyDefinition = String

case class ColumnsDefinition(columns: Map[String, ColumnDefinition])

case class ColumnDefinition(cf: String,
                            col: String,
                            `type`: Option[String],
                            avro: Option[String],
                            length: Option[Int])

case class RegexColumnsDefinition(cf: String,
                                  pattern: String,
                                  `type`: Option[String],
                                  avro: Option[String],
                                  length: Option[Int])