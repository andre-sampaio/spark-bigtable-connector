package com.google.cloud.spark.bigtable.catalog

import com.google.cloud.spark.bigtable.catalog.StringFieldDelimiter.{ByteDelimited, FixedLength, Unbounded}
import com.google.cloud.spark.bigtable.datasources.{AvroSerdes, BigtableTableCatalog, BytesConverter, DataTypeBytes, DataTypeParserWrapper, SchemaConverters}
import org.apache.avro.Schema
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

import java.sql.{Date, Timestamp}

object FieldType {
  def apply(typeString: String): FieldType =
    DataTypeParserWrapper.parse(typeString) match {
      case BooleanType => new BooleanField
      case ByteType => new ByteField
      case ShortType => new ShortField
      case IntegerType => new IntegerField
      case LongType => new LongField
      case FloatType => new FloatField
      case DoubleType => new DoubleField
      case DateType => new DateField
      case TimestampType => new TimestampField
      case StringType => StringField(Unbounded())
      case BinaryType => BinaryField(None)
      case _ => throw new IllegalArgumentException(
        f"$typeString is not a supported type")
    }

  def apply(typeString: String, length: Int): FieldType =
  DataTypeParserWrapper.parse(typeString) match {
    case StringType => StringField(FixedLength(length))
    case BinaryType => BinaryField(Some(length))
    case _ => throw new IllegalArgumentException(
      f"$typeString is not a supported type with length")
  }

  def apply(typeString: String, delimiter: Byte): StringField =
    DataTypeParserWrapper.parse(typeString) match {
      case StringType => StringField(ByteDelimited(delimiter))
      case _ => throw new IllegalArgumentException(
        f"$typeString is not a supported type with length")
    }

  def forAvroSchema(schema: String, sparkColName: String): AvroField =
    AvroField(schema, sparkColName)
}

abstract class FieldType extends Product with Serializable {
  def dataType: DataType
  def toByteArray(input: Any): Array[Byte]
  def fromByteArray(input: Array[Byte], offset: Int): ParsingResult
}

case class ParsingResult(value: Any, consumedBytes: Int)

case class BooleanField() extends FieldType {
  override val dataType: DataType = BooleanType
  override def toByteArray(input: Any): Array[Byte] = BytesConverter.toBytes(input.asInstanceOf[Boolean])
  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult =
    ParsingResult(BytesConverter.toBoolean(input, offset), DataTypeBytes.BOOLEAN_BYTES)
}

case class ByteField() extends FieldType {
  override val dataType: DataType = ByteType
  override def toByteArray(input: Any): Array[Byte] = Array(input.asInstanceOf[Number].byteValue)
  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult =
    ParsingResult(input(offset), DataTypeBytes.BYTE_BYTES)
}

case class ShortField() extends FieldType {
  override val dataType: DataType = ShortType
  override def toByteArray(input: Any): Array[Byte] = BytesConverter.toBytes(input.asInstanceOf[Number].shortValue)
  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult =
    ParsingResult(BytesConverter.toShort(input, offset), DataTypeBytes.SHORT_BYTES)
}

case class IntegerField() extends FieldType {
  override val dataType: DataType = IntegerType
  override def toByteArray(input: Any): Array[Byte] = BytesConverter.toBytes(input.asInstanceOf[Number].intValue)
  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult =
    ParsingResult(BytesConverter.toInt(input, offset), DataTypeBytes.INT_BYTES)
}

case class LongField() extends FieldType {
  override val dataType: DataType = LongType
  override def toByteArray(input: Any): Array[Byte] = BytesConverter.toBytes(input.asInstanceOf[Number].longValue)
  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult =
    ParsingResult(BytesConverter.toLong(input, offset), DataTypeBytes.LONG_BYTES)
}

case class FloatField() extends FieldType {
  override val dataType: DataType = FloatType
  override def toByteArray(input: Any): Array[Byte] = BytesConverter.toBytes(input.asInstanceOf[Number].floatValue)
  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult =
    ParsingResult(BytesConverter.toFloat(input, offset), DataTypeBytes.FLOAT_BYTES)
}

case class DoubleField() extends FieldType {
  override val dataType: DataType = DoubleType
  override def toByteArray(input: Any): Array[Byte] = BytesConverter.toBytes(input.asInstanceOf[Number].doubleValue)
  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult =
    ParsingResult(BytesConverter.toDouble(input, offset), DataTypeBytes.DOUBLE_BYTES)
}

case class DateField() extends FieldType {
  override val dataType: DataType = DateType
  override def toByteArray(input: Any): Array[Byte] = BytesConverter.toBytes(input.asInstanceOf[java.util.Date].getTime)
  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult =
    ParsingResult(new Date(BytesConverter.toLong(input, offset)), DataTypeBytes.LONG_BYTES)
}

case class TimestampField() extends FieldType {
  override val dataType: DataType = TimestampType
  override def toByteArray(input: Any): Array[Byte] = BytesConverter.toBytes(input.asInstanceOf[java.util.Date].getTime)
  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult =
    ParsingResult(new Timestamp(BytesConverter.toLong(input, offset)), DataTypeBytes.LONG_BYTES)
}

case class StringField(delimiter: StringFieldDelimiter) extends FieldType {
  override val dataType: DataType = StringType
  override def toByteArray(input: Any): Array[Byte] = {
    val array = BytesConverter.toBytes(input.toString)
    delimiter match {
      case ByteDelimited(delimiter) =>
        if (array.indexOf(delimiter) < array.length - 1) {
          throw new IllegalArgumentException(
            "Error when writing DataFrame column " + field + " in row "
              + sparkRow + ". " + "When using compound row keys, a String type column "
              + "should have a fixed length or have *exactly one* delimiter character (byte '0') at the end."
          )
        }
      case FixedLength(size) =>
        if (array.size != size) {
          throw new IllegalArgumentException(
            "Error when writing DataFrame column " + field + " in row "
              + sparkRow + ". " + "When using compound row keys, a String type column "
              + "should have a fixed length or have *exactly one* delimiter character (byte '0') at the end."
          )
        }
    }
    array
  }

  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult = delimiter match {
    case FixedLength(length) =>
      ParsingResult(BytesConverter.toString(input.slice(offset, offset + length)), length)
    case ByteDelimited(delimiter) =>
      val pos = input.indexOf(delimiter, offset)
      if (pos == -1) {
        throw new IllegalArgumentException(f"Delimiter not found")
      }
      ParsingResult(BytesConverter.toString(input.slice(offset, pos + 1)), pos - offset)
    case Unbounded() =>
      ParsingResult(BytesConverter.toString(input.drop(offset)), input.length - offset)
  }
}

sealed trait StringFieldDelimiter
object StringFieldDelimiter extends {
  case class FixedLength(size: Int) extends StringFieldDelimiter
  case class ByteDelimited(delimiter: Byte) extends StringFieldDelimiter
  case class Unbounded() extends StringFieldDelimiter
}

case class BinaryField(optLength: Option[Int]) extends FieldType {
  override val dataType: DataType = BinaryType
  override def toByteArray(input: Any): Array[Byte] = input.asInstanceOf[Array[Byte]]
  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult = optLength match {
    case Some(length) =>
      ParsingResult(input.slice(offset, offset + length), length)
    case _ =>
      ParsingResult(input.drop(offset), input.length - offset)
  }
}

case class AvroField(schemaStr: String, sparkColName: String) extends FieldType {
  val schema: Schema = new Schema.Parser().parse(schemaStr)
  override val dataType: DataType = SchemaConverters.toSqlType(schema).dataType

  override def toByteArray(input: Any): Array[Byte] = {
    val record = SchemaConverters.createConverterToAvro(
      dataType, sparkColName, "recordNamespace")(input)
    AvroSerdes.serialize(record, schema)
  }

  override def fromByteArray(input: Array[Byte], offset: Int): ParsingResult = {
    val genericRecord = AvroSerdes.deserialize(input, schema)
    ParsingResult(
      SchemaConverters.createConverterToSQL(schema)(genericRecord),
      input.length)
  }
}
