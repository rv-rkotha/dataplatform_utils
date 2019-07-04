package com.goibibo.dp.utils

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTableUtils._

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

import scala.util.{Try, Failure}

import org.apache.hadoop.fs.Path
import software.amazon.awssdk.services.glue.GlueClient

import org.apache.spark.sql.DataFrame

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import scala.util.{Try, Failure, Success}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object DeltaUtils {

  case class ManifestEntryMetadata(content_length:Long)
  case class ManifestEntries(url:String, meta:ManifestEntryMetadata)
  case class Manifest(entries:Seq[ManifestEntries])

  /** Writes a dataframe to a S3 path, using delta format.
    *
    * This method also ensures that the Glue catalog is updated,
    * and points to an updated list of manifest files,
    * so that data can be queried via Spectrum as well.
    * @param spark Spark Session
    * @param df the dataframe that is to be written
    * @param path the S3 path to be written to
    * @param partitions List of columns to partition data by. Passing None doesn't partition data.
    * @param tb Table Details, consisting of table name and db name
    * @param mergeSchema If true, merges schema in case of compatible schema change, if false, write will fail.
    */
  def writeDF(spark: SparkSession,
            df: DataFrame,
            path: String,
            partitions: Option[Seq[String]] = None,
            tb: GlueUtilsTypes.TableDetails,
            mergeSchema: Boolean = true)(implicit glueClient: GlueClient): Try[Unit] = {

    val normalizedTablePathS3 = normalizeS3Path(path)
    val hadoopPath = new Path(normalizedTablePathS3)
    if (pathExists(spark, hadoopPath)) {
      convertToDelta(spark, hadoopPath, df.schema, partitions)
      deltaToGlue(spark, path, tb.db, tb.name, partitions)
    }

    // Get old schema
    val oldSchema = Try {
      spark.read.format("delta").load(normalizedTablePathS3).schema
    }

    partitions match {
      case None => df.write
          .format("delta")
          .mode("append")
          .option("mergeSchema", mergeSchema.toString)
          .save(path)
      case Some(partitions) => df.write
          .format("delta")
          .partitionBy(partitions:_*)
          .mode("append")
          .option("mergeSchema", mergeSchema.toString)
          .save(path)
    }

    val recreateTable =
      if (mergeSchema && oldSchema.isSuccess) {
          if (!(oldSchema.get == df.schema)) true else false
      } else false

    deltaToGlue(spark, path, tb.db, tb.name, partitions, recreateTable)
  }

  /** Checks if a S3 path is a Delta Table or not, and converts it to a Delta Table.
    *
    * @param spark Spark Session
    * @param path S3 path to convert to Delta
    * @param schema Schema of the data in the delta table
    * @param partitions List of partition columns to partition delta table by.
    */
  def convertToDelta(spark: SparkSession, path: Path, schema: StructType, partitions: Option[Seq[String]]): Unit = {
    if (emptyDir(path) && !pathExists(spark, path)) {
      return
    }
    if (!isDeltaTable(spark, path)) {
      spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
      val convertCommand = s"""
        CONVERT TO DELTA parquet.`${path}/`
        ${partitions match {
        case Some(columns) => "PARTITIONED BY (" ++ schema(columns.toSet).toDDL ++ " )"
        case None => ""}
        }
        """
      spark.sql(convertCommand)
      spark.sql("SET spark.databricks.delta.formatCheck.enabled=true")
    }
  }


  /** Creates or updates a given glue table with information from a delta table.
    *
    * @param spark Spark Session
    * @param path S3 path to delta table
    * @param db database name
    * @param tableName Table Name
    * @param partitions Partitions for the table
    * @param glueClient glueClient to connect to AWS Glue
    */
  def deltaToGlue(spark: SparkSession, path: String,
                  db: String, tableName: String, partitions: Option[Seq[String]] = None,
                  recreateTable: Boolean = false)
                 (implicit glueClient: GlueClient): Try[Unit] = Try {
    // Normalize path so that paths end with /
    val normalizedTablePathS3 = normalizeS3Path(path)
    // Write manifest files for each partition.
    val manifestLocation = writeManifests(spark, normalizedTablePathS3).get

    // Recreate table on schema change.
    // Warning: Dropping and creating table isn't atomic or isolated.
    if (recreateTable) {
      spark.sql(s"""DROP TABLE IF EXISTS ${db}.${tableName}
                """)
    }

    //Create table in glue if it doesn't exist.
    spark.sql(s"""CREATE TABLE IF NOT EXISTS ${db}.${tableName}
                    USING PARQUET
                    LOCATION "${normalizedTablePathS3}"
                    """)

    // Alter table location to point to manifest file(for single partition) or root path(for multiple partitions) .
    val tbDetails = GlueUtilsTypes.TableDetails(tableName, db)
    // TODO: Get partition info from delta log
    GlueUtils.alterTableLocation(tbDetails, manifestLocation, partitions)
  }

  /** Reads Delta Logs for a Delta Table and generates Spectrum-compatible manifest files.
    * Returns a Failure if writes fail, this will leave garbage manifest files.
    * This shouldn't be an issue as long as glue catalog doesn't point to these manifests
    * Returns path to the manifest file if there are no partitions, and path to root folder,
    * if there are partitions.
    *
    * @param spark Spark Session
    * @param path S3 path for Delta Table
    */
  def writeManifests(spark: SparkSession, path: String): Try[String] =  Try {
    val deltaLog = DeltaLog.forTable(spark, path)
    val snapshot = deltaLog.snapshot
    val partitions = snapshot.allFiles
      .collect
      .groupBy(_.partitionValues)
      .map(
        // We write a manifest file for each partition value
        partitionValue => {
          val files = partitionValue._2
          val entries = files.map(
            file => {
              val url = path + file.path.toString
              val meta = ManifestEntryMetadata(file.size)
              ManifestEntries(url, meta)
            }
          )
          val submanifest = Manifest(entries)

          implicit val formats = Serialization.formats(NoTypeHints)
          val submanifestJson = write(submanifest)
          val MANIFEST_LOCATION = "_manifests/"
          val partitionPath = partitionValue._1.map{case (k, v) => k + "=" + v + "/" }.mkString
          val submanifestLocation = path + partitionPath + MANIFEST_LOCATION  + System.currentTimeMillis.toString
          val S3WriteSucceeded = dbutils.fs.put(submanifestLocation, submanifestJson)
          if (!S3WriteSucceeded) {
            return Failure(new Exception("Failed to write manifest to S3"))
          }
          submanifestLocation
        }
      )

    if(partitions.size == 1) {
      return Success(partitions.head)
    } else return Success(path)
  }

  /** Checks if a path exists in S3
    * @param spark Spark session
    * @param path S3 path
    */
  private def pathExists(spark: SparkSession, path: Path): Boolean = {
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    Try(fs.exists(path)).getOrElse(false)
  }

  /** Checks if a directory(S3 prefix) is empty
    * @param path S3 path
    */
  private def emptyDir(path: Path) : Boolean = {
    Try(dbutils.fs.ls(path.toUri().toString).size).getOrElse(0) != 0
  }

  /** Checks for trailing slash in S3 path and adds one if missing
    * @param path S3 path
    */
  private def normalizeS3Path(path: String): String = {
    val normalizedPath = if (path.endsWith("/")) path else {path + "/"}
    normalizedPath
  }
}
