package com.goibibo.dp.utils

import com.amazonaws.services.glue.AWSGlueClient
import com.amazonaws.services.glue.model.{GetTableRequest, UpdateTableRequest,TableInput}
import com.amazonaws.regions.{Region,Regions}

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

import scala.util.Try

case class ManifestEntryMetadata(content_length:Long)
case class ManifestEntries(url:String, meta:ManifestEntryMetadata)
case class Manifest(entries:Seq[ManifestEntries])

object DeltaUtils {

  def deltaToGlue(spark: SparkSession, tablePathS3: String,
                  db: String, tableName: String): Try[Unit] = {

    val deltaLog = DeltaLog.forTable(spark, tablePathS3)

    /*
     DeltaLog internally uses snapshot to get the list of files,
     https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/Snapshot.scala
     */
    val snapshot = deltaLog.snapshot
    val currentSnapShotFiles = snapshot.allFiles.collect

    val entries = currentSnapShotFiles.map(
      a => {
        val url = tablePathS3 + a.path.toString
        val meta = ManifestEntryMetadata(a.size)
        ManifestEntries(url, meta)
      })
    val manifest = Manifest(entries)

    implicit val formats = Serialization.formats(NoTypeHints)
    val manifestJson = write(manifest)
    val MANIFEST_LOCATION = "_manifests/"
    val manifestLocation = tablePathS3 + MANIFEST_LOCATION  + System.currentTimeMillis.toString
    dbutils.fs.put(manifestLocation, manifestJson)

    alterTableLocation(db, tableName, manifestLocation)
  }

  /* TODO: Use GlueUtils's function instead */
  private def alterTableLocation(db: String, tableName: String,
                         manifestLocation: String,
                         region: String = "ap-south-1"): Try[Unit] = Try {
    val glueClient = AWSGlueClient.builder().withRegion(region).build()
    val getTableRequest = new GetTableRequest().withDatabaseName(db).withName(tableName)
    val getTableResp = glueClient.getTable(getTableRequest)
    val table = getTableResp.getTable()

    val storageDescriptor = table.getStorageDescriptor().withLocation(manifestLocation)
    val tableInput = new TableInput().
      withName(table.getName()).
      withTableType(table.getTableType()).
      withPartitionKeys(table.getPartitionKeys()).
      withStorageDescriptor(storageDescriptor)
    val updateTableRequest = new UpdateTableRequest().withDatabaseName(db).withTableInput(tableInput)
    glueClient.updateTable(updateTableRequest)
  }
}
