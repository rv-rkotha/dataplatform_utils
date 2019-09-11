package com.goibibo.dp.utils

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTableUtils._

import scala.util.{Try, Failure}
import org.apache.hadoop.fs.Path
import software.amazon.awssdk.services.glue.GlueClient

import scala.collection.mutable

import java.util.concurrent.TimeUnit._

import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.types.{StructType, DataType}
import org.apache.spark.sql.catalyst.analysis

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import scala.util.{Try, Failure}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.sql.catalyst.catalog.{CatalogUtils, CatalogTable, CatalogTableType, CatalogTablePartition, CatalogStorageFormat}
import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.hadoop.hive.metastore.api.{Partition, StorageDescriptor, SerDeInfo}
import collection.JavaConverters._

import org.apache.spark.sql.delta.storage.LogStore

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

case class ManifestEntryMetadata(content_length:Long)
case class ManifestEntries(url:String, meta:ManifestEntryMetadata)
case class Manifest(entries:Seq[ManifestEntries])

object DeltaUtils {
  def writeDF(spark: SparkSession,
              df: DataFrame,
              path: String,
              partitions: Option[Seq[String]] = None,
              tb: GlueUtilsTypes.TableDetails,
              mergeSchema: Boolean = true)(implicit glueClient: GlueClient): Try[Unit] = Try {

    val normalizedTablePathS3 = normalizeS3Path(path)
    val hadoopPath = new Path(normalizedTablePathS3)
    if (pathExists(spark, hadoopPath)) {
      convertToDelta(spark, hadoopPath, df.schema, partitions)
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

    deltaToGlue(spark, path, tb.db, tb.name, mergeSchema, partitions).get
  } recoverWith {
    case t: Throwable => t.printStackTrace; Failure(t)
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
                  dbName: String, tableName: String,
                  mergeSchema: Boolean = true,
                  partitionColumns: Option[Seq[String]] = None,
                  tableProperties: Map[String, String] = Map.empty[String, String])
                 (implicit glueClient: GlueClient): Try[Unit] = Try {
    import spark.implicits._

    // Normalize path so that paths end with /
    val normalizedTablePathS3 = normalizeS3Path(path)

    val catalog = spark.sharedState.externalCatalog
    val msc = org.apache.spark.sql.hive.GoHiveUtils.getMSC(spark).get
    val deltaLog = DeltaLog.forTable(spark, path)
    val latestDeltaVersion = deltaLog.history.getHistory(Some(1))(0).version.get

    val(lastWrittenDeltaVersion, oldSchema) = if (catalog.tableExists(dbName, tableName)) {
      val table = catalog.getTable(dbName, tableName)
      val lastWrittenDeltaVersion = Try{
        Some(table.properties("deltaVersion").toLong)
      }.getOrElse(None)
      val oldSchema = Some(table.schema)
      (lastWrittenDeltaVersion, oldSchema)
    } else (None, None)
    println(s"Last Glue Delta Version: ${lastWrittenDeltaVersion}")
    println(s"Latest Delta Version: ${latestDeltaVersion}")
    println(s"Old Schema: ${oldSchema}")

    // Write manifest files for each partition.
    val partResp = writeManifests(spark, normalizedTablePathS3,
                                  (lastWrittenDeltaVersion.map(_+1),
                                   latestDeltaVersion),
                                  tryIncremental = true).get
    val catalogPartitions = partResp.map{case(cp, l) => cp}

    val schema = spark.table(s"delta.`${normalizedTablePathS3}`").schema
    println(s"New Schema: ${schema}")
    // Infer partition columns from delta if not provided.
    val partitionColumnNames: Seq[String] = partitionColumns.getOrElse(
      spark.sql(s"describe DETAIL delta.`${normalizedTablePathS3}`")
        .limit(1)
        .select("partitionColumns")
        .as[Seq[String]].collect.head)

    // Create table in glue if it doesn't exist.
    val serde = org.apache.spark.sql.internal.HiveSerDe.serdeMap.get("parquet").get
    val placeholderPath = normalizedTablePathS3 +"_non_existent/"
    val catalogTable = CatalogTable(
      identifier = TableIdentifier(tableName, Some(dbName)),
      tableType = CatalogTableType("EXTERNAL"),
      // Table points to a non-existent location by default,
      // and will be updated with a manifest file for non-partitioned tables.
      storage = CatalogStorageFormat(
        locationUri = Some(new java.net.URI(placeholderPath)),
        inputFormat = serde.inputFormat,
        outputFormat = serde.outputFormat,
        serde = serde.serde,
        compressed = false,
        properties =  Map("path" -> placeholderPath)
      ),
      schema = schema,
      provider = Some("hive"),
      partitionColumnNames = partitionColumnNames,
      tracksPartitionsInCatalog = true,
      properties = tableProperties + ("deltaVersion" -> latestDeltaVersion.toString)
    )
    catalog.createTable(catalogTable, ignoreIfExists = true)

    val partitions = partResp.map { case (cp, l) =>
      toHivePartition(spark, cp, catalogTable, dbName, tableName, Some(l)).get
    }

    val alterSchema =
      if (mergeSchema && !oldSchema.isEmpty) {
        if (!DataType.canWrite(schema, oldSchema.get,
                               analysis.caseInsensitiveResolution,
                               "record")) true else false
      } else false

    if (alterSchema) {
      val hiveSchemaFields = schema.filter(sf => !(partitionColumnNames.contains(sf.name)))
      catalog.alterTableDataSchema(dbName, tableName, StructType(hiveSchemaFields))
    }

    if (partitionColumnNames.isEmpty) {
      // Set table location if there's no partitions.
      // This cannot be done through the catalog interface,
      // as hive doesn't support manifest files for location.
      val tbDetails = GlueUtilsTypes.TableDetails(tableName, dbName)
      val manifestLocation = partResp.map{case(cp, l) => l}.head
      GlueUtils.alterTableLocation(tbDetails, manifestLocation, None).get
    } else {
      // Create or replace partitions.
      // All partitions have their locations updated to the new manifest file.
      catalog.createPartitions(dbName, tableName, catalogPartitions, ignoreIfExists = true)
      // Set partition locations. This cannot be done through the catalog interface,
      // as hive doesn't support manifest files for location.
      msc.alter_partitions(dbName, tableName, partitions.asJava)
      // Alter table properties to set deltaVersion
      // Only needed for partitioned tables.
      catalog.alterTable(catalogTable)
    }
  } recoverWith {
    case t: Throwable => t.printStackTrace(); Failure(t)
  }

  /** Reads Delta Logs for a Delta Table and generates Spectrum-compatible manifest files.
    * Returns a Failure if writes fail, this will leave garbage manifest files.
    * This shouldn't be an issue as long as glue catalog doesn't point to these manifests
    * Returns path to the manifest file if there are no partitions, and path to root folder,
    * if there are partitions.
    *
    * @param spark Spark Session
    * @param path S3 path for Delta Table
    * @deltaVersionRange If render
    */
  def writeManifests(spark: SparkSession, path: String, deltaVersionRange: (Option[Long], Long), tryIncremental: Boolean = true) = Try {
    import spark.implicits._

    val latestDeltaVersion = deltaVersionRange._2
    val startingDeltaVersion = deltaVersionRange._1.getOrElse(latestDeltaVersion)

    var proceedWithIncremental = tryIncremental
    if (!isRangeValid(spark, path, deltaVersionRange)) {
      println("WARNING: Unable to run manifests incrementally. Writing it for all partitions.")
      proceedWithIncremental = false
    }

    val files = (startingDeltaVersion to latestDeltaVersion).map( v =>
      f"${path}/_delta_log/$v%020d.json"
    )

    def to_sorted_str = (s: Map[String, String]) => {
      s.toSeq.sorted.toMap.toString
    }

    val to_str = spark.udf.register("to_str", to_sorted_str)
    val partitionsChanged = if (proceedWithIncremental) Try {
      spark.read.json(files: _*)
        .where("add is not null")
        .select(to_json($"add.partitionValues").as("jv"))
        .select(from_json($"jv", MapType(StringType, StringType, false)).as("mv"))
        .select(to_str($"mv"))
        .as[String]
        .collect
        .distinct
    }.toOption else None

    println("Changed partitions:")
    partitionsChanged.getOrElse(Array()).foreach(p => println(p))

    val deltaLog = DeltaLog.forTable(spark, path)
    val allPartitionValues = deltaLog.snapshot.allFiles
      .selectExpr("to_json(partitionValues) as partition_values", "path", "size")

    val filteredPartitionValues = if (partitionsChanged.isEmpty) allPartitionValues
                                  else {
                                    allPartitionValues.where(to_str($"partitionValues").isin(partitionsChanged.get: _*))
                                  }

    println("Writing to partitions:")
    filteredPartitionValues.collect.foreach(p => println(p))

    val partitionData =
      filteredPartitionValues
        .groupBy("partition_values")
        .agg(collect_list(struct($"path", $"size")).as("files"))
        .select(from_json($"partition_values", MapType(StringType, StringType, false)), $"files")
        .as[(Map[String, String], Seq[(String, Long)])]
        .map( pv =>
          pv match {
            case (partitionValue: Map[String, String], files: Seq[(String, Long)]) => {
              val subManifestJson = getManifestContent(files, path)
              val(partitionLocation, subManifestLocation) = writeManifest(partitionValue, subManifestJson, path, deltaVersionRange._2)
              (partitionValue, partitionLocation, subManifestLocation)
            }
          }
        )
    val serde = org.apache.spark.sql.internal.HiveSerDe.serdeMap.get("parquet").get
    val catalogPartitions = partitionData.collect.map{ case (partitionValue, partitionLocation, subManifestLocation)  =>
      val catalogPartition = CatalogTablePartition(partitionValue,
                                                   CatalogStorageFormat(Some(new java.net.URI(partitionLocation + "_non_existent/")),
                                                                        serde.inputFormat,
                                                                        serde.outputFormat,
                                                                        serde.serde,
                                                                        false,
                                                                        Map.empty[String, String]
                                                   )
      )
      (catalogPartition, subManifestLocation)
    }
    catalogPartitions.toSeq
  }  recoverWith {
    case t: Throwable => t.printStackTrace; Failure(t)
  }

  private def isRangeValid(spark: SparkSession, path: String, deltaVersionRange: (Option[Long], Long)): Boolean = {
    if (deltaVersionRange._1.isEmpty) return false
    val startingDeltaVersion = deltaVersionRange._1.get
    val lastDeltaVersion = deltaVersionRange._2
    if (startingDeltaVersion > lastDeltaVersion) return false
    val deltaFiles = LogStore(spark.sparkContext)
      .listFrom(f"${path}/_delta_log/$startingDeltaVersion%020d.json")
      .map(p => p.getPath.toString)
      .filter(p => p.endsWith(".json"))
      .toList
    val files = (startingDeltaVersion to lastDeltaVersion).map( v =>
      new Path(f"${path}/_delta_log/$v%020d.json").toString
    ).toList
    deltaFiles.containsSlice(files)
  }

  private def getManifestContent(files: Seq[(String, Long)], path:String) =  {
    val entries = files.map{ file =>
      val url = path + file._1
      val meta = ManifestEntryMetadata(file._2)
      ManifestEntries(url, meta)
    }
    val submanifest = Manifest(entries)
    implicit val formats = Serialization.formats(NoTypeHints)
    write(submanifest)
  }

  private def writeManifest(partitionValue: Map[String, String], manifestJson: String, path: String, deltaVersion: Long): (String, String) = {
    val partitionPath = partitionValue.map{case (k, v) => println(k); println(v); s"${k}=${v}/"}.mkString
    val partitionLocation = path + partitionPath
    val MANIFEST_DIR = "_manifests/"
    val manifestLocation = partitionLocation + MANIFEST_DIR + deltaVersion
    val hadoopPath = new Path(manifestLocation)
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopPath.toUri, new org.apache.hadoop.conf.Configuration())
    val fileStream = fs.create(hadoopPath, true)
    fileStream.write(manifestJson.getBytes)
    fileStream.close
    (partitionLocation, manifestLocation)
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

  private def toHivePartition(spark: SparkSession, p: CatalogTablePartition, ct: CatalogTable, dbName:String, tableName: String, location: Option[String]): Try[Partition] = Try {
    val tpart = new org.apache.hadoop.hive.metastore.api.Partition
    val ht = org.apache.spark.sql.hive.GoHiveUtils.toHiveTable(spark, ct).get
    val partValues = ht.getPartCols.asScala.map { hc =>
      p.spec.getOrElse(hc.getName,
                       throw new IllegalArgumentException(
                         s"Partition spec is missing a value for column '${hc.getName}': ${p.spec}")
      )
    }
    val storageDesc = new StorageDescriptor
    val serdeInfo = new SerDeInfo
    location match {
      case None => p.storage.locationUri.map(CatalogUtils.URIToString(_)).foreach(storageDesc.setLocation)
      case Some(l) => storageDesc.setLocation(l)
    }
    p.storage.inputFormat.foreach(storageDesc.setInputFormat)
    p.storage.outputFormat.foreach(storageDesc.setOutputFormat)
    p.storage.serde.foreach(serdeInfo.setSerializationLib)
    serdeInfo.setParameters(p.storage.properties.asJava)
    storageDesc.setSerdeInfo(serdeInfo)
    storageDesc.setCols(ht.getCols)
    storageDesc.setBucketCols(ht.getBucketCols)
    tpart.setDbName(dbName)
    tpart.setTableName(tableName)
    tpart.setValues(partValues.asJava)
    tpart.setSd(storageDesc)
    tpart.setCreateTime(MILLISECONDS.toSeconds(p.createTime).toInt)
    tpart.setLastAccessTime(MILLISECONDS.toSeconds(p.lastAccessTime).toInt)
    tpart.setParameters(mutable.Map(p.parameters.toSeq: _*).asJava)
    tpart
  } recoverWith {
    case t: Throwable => t.printStackTrace; Failure(t)
  }
}
