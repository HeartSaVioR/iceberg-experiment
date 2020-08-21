package net.heartsavior.iceberg

import java.util

import scala.collection.JavaConverters._

import org.apache.iceberg.Schema
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.types.Types
import org.apache.iceberg.hive.HiveCatalog

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

object ExperimentBatchQuery {
  private def createTable(catalog: HiveCatalog, tableIdentifier: TableIdentifier, location: String): Table = {
    val schema = new Schema(
      Types.NestedField.required(1, "level", Types.StringType.get()),
      Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
      Types.NestedField.required(3, "message", Types.StringType.get()))

    val spec = PartitionSpec.builderFor(schema)
      .hour("event_time")
      .identity("level")
      .build();

    catalog.createTable(tableIdentifier, schema, spec, location, Map.empty[String, String].asJava)
  }

  private def printSnapshots(table: Table): Unit = {
    table.history().asScala.foreach { historyEntry =>
      println(s"ID: ${historyEntry.snapshotId()}, Timestamp: ${historyEntry.timestampMillis()}")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ExperimentBatchQuery")
      .getOrCreate()

    if (args.length != 3) {
      println("USAGE: [databaseName] [tableName] [location]")
      sys.exit(1)
    }

    val databaseName = args(0)
    val tableName = args(1)
    val location = args(2)

    println(s"database name: $databaseName")
    println(s"table name: $tableName")
    println(s"location: $location")

    println("Setting up Hive Catalog...")

    val catalog = new HiveCatalog(spark.sparkContext.hadoopConfiguration)

    val tableIdentifier = TableIdentifier.of(databaseName, tableName)

    println("Creating table...")

    val table = createTable(catalog, tableIdentifier, location)
    table.refresh()

    println("========================= Before appending ========================")
    printSnapshots(table)

    println("Writing data into table...")

    // level: String, event_time: Timestamp, message: String
    val dataArray = new util.ArrayList[Row]()
    val currentTimeSec = System.currentTimeMillis() / 1000
    Seq(
      ("INFO", currentTimeSec, "message 1"),
      ("WARN", currentTimeSec, "message 2"),
      ("DEBUG", currentTimeSec, "message 3"),
      ("DEBUG", currentTimeSec, "message 4"),
      ("INFO", currentTimeSec, "message 5")
    ).map { case (level, eventTime, message) =>
      dataArray.add(new GenericRow(Array(level, eventTime, message)))
    }

    val schema = StructType(
      StructField("level", StringType, nullable = false) ::
        StructField("event_time_long", LongType, nullable = false) ::
        StructField("message", StringType, nullable = false) :: Nil)

    val df = spark.createDataFrame(dataArray, schema)
    df.selectExpr("level", "CAST(event_time_long AS timestamp) AS event_time", "message")
      .write
      // HiveConf.ConfVars.METASTOREURIS.varname should be set to make this work
      .format("iceberg")
      .mode("append")
      .save(tableIdentifier.toString)

    table.refresh()

    println("========================= After appending ========================")
    printSnapshots(table)

    println("Reading data from table...")

    spark.read
      .format("iceberg")
      .load(tableIdentifier.toString)
      .write
      .format("console")
      .save()
  }
}
