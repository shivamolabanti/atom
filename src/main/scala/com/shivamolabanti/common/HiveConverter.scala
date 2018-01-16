package com.shivamolabanti.common

import org.apache.spark.sql.types._

/**
  * Created by shiva on 3/23/17.
  */
object HiveConverter {

  val primitiveTypes =
    Seq(StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, ByteType,
      ShortType, DateType, TimestampType, BinaryType)

  /**
    * Convert the Spark schema to a HIVE one.
    *
    * @param schema
    * @return
    */
  def convertSchema(schema: StructType): String = {
    schema.map { f =>
      val t = getHiveType(f.dataType)
      s"${f.name} $t"
    }.mkString(", ")
  }

  def getHiveType(dataType: DataType): String = {
    dataType match {
      case StringType => "STRING"
      case IntegerType => "INT"
      case LongType => "BIGINT"
      case DoubleType => "DOUBLE"
      case FloatType => "FLOAT"
      case BooleanType => "BOOLEAN"
      case ByteType => "TINYINT"
      case ShortType => "SMALLINT"
      case DateType => "DATE"
      case TimestampType => "TIMESTAMP"
      case BinaryType => "BINARY"

      case ArrayType(at, _) =>
        val t = getHiveType(at)
        s"ARRAY<$t>"

      case MapType(kt, vt, _) =>
        val k = getHiveType(kt)
        val v = getHiveType(vt)
        s"MAP<$k, $v>"

      case StructType(fields) =>
        val sf = fields.map(f => s"${f.name}: ${getHiveType(f.dataType)}").mkString(", ")
        s"STRUCT<$sf>"

      case _ => throw new Exception(s"Unhandled data type of ${dataType.toString}")
    }
  }

  /**
    * Get the HQL to create a table
    *
    * @param partitionColumns
    * @param hiveDb
    * @param hiveTable
    * @param schema
    * @return
    */
  def getHiveCreate(partitionColumns: Set[String], hiveDb: String, hiveTable: String, schema: StructType): String = {
    // We need get a Hive compatible schema so the table can be created if it does not exist. We need
    // to do this because calling `partitionBy` on the DataFrame will not create a compatable table
    val parts = partitionColumns.map { c =>
      val f = HiveConverter.getHiveType(schema.filter(_.name.equalsIgnoreCase(c)).head.dataType)
      s"$c $f"
    }.mkString(",")

    // Get the Hive schema without any partition columns since Hive needs those in the partition section and can't
    // be duplicated
    val hiveSchema = HiveConverter.convertSchema(
      StructType(schema.fields.filterNot(f => partitionColumns.filter(_.equalsIgnoreCase(f.name)).headOption.isDefined)))


    /*s"CREATE TABLE IF NOT EXISTS $hiveDb.$hiveTable ($hiveSchema) PARTITIONED BY ($parts) STORED AS ORC " +
      "TBLPROPERTIES(\"orc.compress\"=\"SNAPPY\")"*/

    s"CREATE TABLE IF NOT EXISTS $hiveDb.$hiveTable ($hiveSchema) PARTITIONED BY ($parts) STORED AS ORC "
  }


}
