package com.goibibo.dp.utils
import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import DeltaUtils._

class DeltaUtilsTest extends FlatSpec with Matchers with PrivateMethodTester with SharedSparkContext {

  "normalizePath" should "ensure / at end" in {
    val normalizeS3Path = PrivateMethod[String]('normalizeS3Path)
    val resultWithoutSlash = DeltaUtils invokePrivate normalizeS3Path("s3://path/test")
    assert(resultWithoutSlash == "s3://path/test/")

    val resultWithSlash = DeltaUtils invokePrivate normalizeS3Path("s3://path/test/")
    assert(resultWithSlash == "s3://path/test/")
  }

  "Extraction of db and table name" should "be correct" in {
    val getTableDetails = PrivateMethod[(String, String)]('getTableDetails)
    val tbResult = DeltaUtils invokePrivate getTableDetails("test.table")
    assert(tbResult == ("test", "table"))
    assertThrows[Exception]{
      DeltaUtils invokePrivate getTableDetails("test.table.withdot")
    }
    assertThrows[Exception]{
      DeltaUtils invokePrivate getTableDetails("test")
    }
  }

  "path existence" should "be correct" in {
    val sparkSession = SparkSession
      .builder()
      .config(sc.getConf)
      .getOrCreate()

    val pathExists = PrivateMethod[Boolean]('pathExists)

    val resultExists = DeltaUtils invokePrivate pathExists(sparkSession,
                                                           new Path("s3a://goibibo-data-platform-dev/crawl/test_data/"))
    val resultNotExists = DeltaUtils invokePrivate pathExists(sparkSession,
                                                              new Path("s3a://goibibo-data-platform-dev/crawl/test_data/arbitrary_string"))

    assert(resultExists == true)
    assert(resultNotExists == false)
  }

  "DeltaToGlue" should "run for unpartitioned table via path" in {
    val sparkSession = SparkSession
      .builder()
      .config(sc.getConf)
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    val path = "s3a://goibibo-data-platform-dev/crawl/delta_test1/"
    implicit val glueClient = GlueUtils.createGlueClient()
    S3Utils.deleteS3Data(path)

    val df = Seq(("Rey", 12, 21),
                 ("John", 34, 44),
                 ("Neal", 43, 44),
                 ("Steve", 12, 52),
                 ("Phil", 15, 23)).toDF("name","score", "age")
    df.write.format("delta").mode("overwrite").save(path)
    deltaToGlue(sparkSession, DPath(path), STable("goibibo_e.delta_test1")).get
    S3Utils.deleteS3Data(path)
    sparkSession.sharedState.externalCatalog.dropTable("goibibo_e", "delta_test1", ignoreIfNotExists = true, purge = true)
  }

  "DeltaToGlue" should "run for unpartitioned table via table" in {
    val sparkSession = SparkSession
      .builder()
      .config(sc.getConf)
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    val table = "goibibo_e.delta_test2"
    implicit val glueClient = GlueUtils.createGlueClient()
    sparkSession.sharedState.externalCatalog.dropTable("goibibo_e", "delta_test2", ignoreIfNotExists = true, purge = true)
    sparkSession.sharedState.externalCatalog.dropTable("goibibo_e", "delta_test20", ignoreIfNotExists = true, purge = true)

    val df = Seq(("Rey", 12, 21),
                 ("John", 34, 44),
                 ("Neal", 43, 44),
                 ("Steve", 12, 52),
                 ("Phil", 15, 23)).toDF("name","score", "age")
    df.write.format("delta").mode("overwrite").saveAsTable(table)
    deltaToGlue(sparkSession, DTable(table), STable("goibibo_e.delta_test20")).get
    sparkSession.sharedState.externalCatalog.dropTable("goibibo_e", "delta_test2", ignoreIfNotExists = true, purge = true)
    sparkSession.sharedState.externalCatalog.dropTable("goibibo_e", "delta_test20", ignoreIfNotExists = true, purge = true)
  }

  "DeltaToGlue" should "run after multiple writes" in {
    val sparkSession = SparkSession
      .builder()
      .config(sc.getConf)
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    val path = "s3a://goibibo-data-platform-dev/crawl/delta_test3/"
    implicit val glueClient = GlueUtils.createGlueClient()
    S3Utils.deleteS3Data(path)

    val df = Seq(("Rey", 12, 21),
                 ("John", 34, 44),
                 ("Neal", 43, 44),
                 ("Steve", 12, 52),
                 ("Phil", 15, 23)).toDF("name","score", "age")
    df.write.format("delta").mode("append").save(path)
    df.write.format("delta").mode("append").save(path)
    deltaToGlue(sparkSession, DPath(path), STable("goibibo_e.delta_test3")).get
    S3Utils.deleteS3Data(path)
    sparkSession.sharedState.externalCatalog.dropTable("goibibo_e", "delta_test3", ignoreIfNotExists = true, purge = true)
  }

  "DeltaToGlue" should "run for partitioned tables" in {
    val sparkSession = SparkSession
      .builder()
      .config(sc.getConf)
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    val path = "s3a://goibibo-data-platform-dev/crawl/delta_test4/"
    implicit val glueClient = GlueUtils.createGlueClient()
    S3Utils.deleteS3Data(path)

    val df = Seq(("Rey", 12, 21),
                 ("John", 34, 44),
                 ("Neal", 43, 44),
                 ("Steve", 12, 52),
                 ("Phil", 15, 23)).toDF("name","score", "age")
    df.write.format("delta").partitionBy("age").mode("append").save(path)
    deltaToGlue(sparkSession, DPath(path), STable("goibibo_e.delta_test4")).get
    S3Utils.deleteS3Data(path)
    sparkSession.sharedState.externalCatalog.dropTable("goibibo_e", "delta_test4", ignoreIfNotExists = true, purge = true)
  }

  "DeltaToGlue" should "run on non-empty table" in {
    // TODO: Check for incremental run
    val sparkSession = SparkSession
      .builder()
      .config(sc.getConf)
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    val path = "s3a://goibibo-data-platform-dev/crawl/delta_test5/"
    implicit val glueClient = GlueUtils.createGlueClient()
    S3Utils.deleteS3Data(path)

    val df = Seq(("Rey", 12, 21),
                 ("John", 34, 44),
                 ("Neal", 43, 44),
                 ("Steve", 12, 52),
                 ("Phil", 15, 23)).toDF("name","score", "age")
    df.write.format("delta").mode("append").save(path)
    deltaToGlue(sparkSession, DPath(path), STable("goibibo_e.delta_test5")).get
    df.write.format("delta").mode("append").save(path)
    deltaToGlue(sparkSession, DPath(path), STable("goibibo_e.delta_test5")).get
    S3Utils.deleteS3Data(path)
    sparkSession.sharedState.externalCatalog.dropTable("goibibo_e", "delta_test5", ignoreIfNotExists = true, purge = true)
  }
}
