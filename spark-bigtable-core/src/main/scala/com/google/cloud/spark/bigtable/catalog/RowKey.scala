package com.google.cloud.spark.bigtable.catalog

import com.google.cloud.spark.bigtable.datasources.Field
import org.apache.yetus.audience.InterfaceAudience

// The row key definition, with each key refer to the col defined in Field, e.g.,
// key1:key2:key3
@InterfaceAudience.Private
case class RowKey(fields: Seq[Field]) {
  val keys = k.split(":")
  var fields: Seq[Field] = _
  var varLength = false
  def length = {
    if (varLength) {
      -1
    } else {
      fields.foldLeft(0) { case (x, y) =>
        x + y.length
      }
    }
  }
}

object RowKey {
  def apply(params: Map[String, String]): RowKey = {

  }
}
