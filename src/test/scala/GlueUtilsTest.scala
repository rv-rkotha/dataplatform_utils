package com.goibibo.dp.utils
import GlueUtilsTypes._
import org.scalatest._
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter}
import org.slf4j.Logger
import pureconfig.loadConfigOrThrow
import resource.managed
import org.slf4j.{Logger, LoggerFactory}
import java.util.{Calendar,TimeZone}

class GlueUtilsTest extends FlatSpec with Matchers {

  import pureconfig.generic.auto._
  final case class RedshiftConf(  clusterId:String,user:String, dbGroup:String, autoCreate:String, region:String)
  val redshiftConf = loadConfigOrThrow[RedshiftConf]("redshift")
  import redshiftConf._
  implicit val redshiftClient = RedshiftUtil.getClusterClient(region)
  val utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "GlueType" should "return valid types for premitive types" in {
    GlueUtilsPrivate.glueType[String]() should be ("STRING")
    GlueUtilsPrivate.glueType[Int]() should be ("INT")
    GlueUtilsPrivate.glueType[Float]() should be ("FLOAT")
    GlueUtilsPrivate.glueType[Double]() should be ("DOUBLE")
    GlueUtilsPrivate.glueType[java.util.Date]() should be ("DATE")
  }

  "FieldInfoValue" should "auto detect field type" in {
    val f = FieldInfoValue("year", 2018 )
    f.fieldType should be ("INT")
  }
  
  "creation of GlueClient" should "successful" in {
    val client = GlueUtils.createGlueClient()
    client.close
  }

  "creation and drop of the Table" should "be successful" in {
    implicit val client = GlueUtils.createGlueClient()
    val tableDetails  = TableDetails("testsunny","goibibo_e")
//    val partitionKeys = Seq(FieldInfo("year","Int"))
    val partitionKeys = Seq[FieldInfo]()
    val fields        = Seq(
                          FieldInfo("id","string"),
                          FieldInfo("name","string"),
                          FieldInfo("created","timestamp")
                        )

    val location      = "s3://tmp-data-platform/test-sunny/"
    val path = location.replace("s3","s3a") + "test.parquet"
    S3Utils.deleteS3Data(path)
    val users = getDummyUsersData
    writeData(path, users)

    GlueUtils.dropTable(tableDetails)
    GlueUtils.createTable(tableDetails,partitionKeys, fields, location,ParquetFormat).get

    assert(GlueUtils.doesTableExist(tableDetails).get == true)

    var usersR = Seq[User]()
    for(
      connection <- managed(RedshiftUtil.getConnection(clusterId,user).get);
      rs <- RedshiftUtil.executeQuery(connection, s"select id, name, created from ${tableDetails.db}.${tableDetails.name}")
    ) {
      while(rs.next()) {
        val id = rs.getString("id")
        val name = rs.getString("name")
        //If we don't specify UTC-Calendar then JDBC adjusts the epoch with default timezone, Crazy stuff.
        //https://stackoverflow.com/a/34172897/148869
        val created = rs.getTimestamp("created",utcCal)
        usersR +:= User(id, name, created)
      }
    }
    assert(usersR.toList.size == 2)
    assert(usersR.toList.sortBy(_.id).map(_.created.getTime) == users.toList.sortBy(_.id).map(_.created.getTime))

    GlueUtils.dropTable(tableDetails)
    assert(GlueUtils.doesTableExist(tableDetails).get == false)
    S3Utils.deleteS3Data(path)
    client.close
  }

  "Alter table" should "be successful" in {
    implicit val client = GlueUtils.createGlueClient()
    val tableDetails  = TableDetails("testsunny","goibibo_e")
    //    val partitionKeys = Seq(FieldInfo("year","Int"))
    val partitionKeys = Seq[FieldInfo]()
    val fields        = Seq(
      FieldInfo("id","string"),
      FieldInfo("name","string"),
      FieldInfo("created","timestamp")
    )

    val location      = "s3://tmp-data-platform/test-sunny/"
    val path = location.replace("s3","s3a") + "test.parquet"
    S3Utils.deleteS3Data(path)

    val users = getDummyUsersData
    writeData(path, users)

    GlueUtils.dropTable(tableDetails)
    GlueUtils.createTable(tableDetails,partitionKeys, fields, location,ParquetFormat).get

    assert(GlueUtils.doesTableExist(tableDetails).get == true)
    val modifiedUsers = getDummyUsersData.filter(_.name == "Sunny")
    val newLocation      = "s3://tmp-data-platform/test-sunny_1/"
    val newPath = newLocation.replace("s3","s3a") + "test.parquet"
    S3Utils.deleteS3Data(newPath)
    writeData(newPath, modifiedUsers)
    GlueUtils.alterTableLocation(tableDetails, newLocation)

    var usersR = Seq[User]()
    for(
      connection <- managed(RedshiftUtil.getConnection(clusterId,user).get);
      rs <- RedshiftUtil.executeQuery(connection, s"select id, name, created from ${tableDetails.db}.${tableDetails.name}")
    ) {
      while(rs.next()) {
        val id = rs.getString("id")
        val name = rs.getString("name")
        //If we don't specify UTC-Calendar then JDBC adjusts the epoch with default timezone, Crazy stuff.
        //https://stackoverflow.com/a/34172897/148869
        val created = rs.getTimestamp("created",utcCal)
        usersR +:= User(id, name, created)
      }
    }
    assert(usersR.toList.size == 1)
    assert(usersR.toList.sortBy(_.id) == modifiedUsers.toList.sortBy(_.id))

    S3Utils.deleteS3Data(newPath)
    S3Utils.deleteS3Data(path)
    GlueUtils.dropTable(tableDetails)
    assert(GlueUtils.doesTableExist(tableDetails).get == false)
    client.close
  }

  "Read and Write to/From S3 of parquet file" should "successful" in {


    val users: Stream[User] = getDummyUsersData

    val path = "s3a://tmp-data-platform/test-sunny/kjsdf.parquet"
    S3Utils.deleteS3Data(path)
    writeData(path,users)
    val usersR = ParquetReader.read[User](path)
    assert(usersR.toSeq.sortBy(_.id) == users.sortBy(_.id))
    S3Utils.deleteS3Data(path)
  }

  case class User(id: String, name: String, created: java.sql.Timestamp)

  def getDummyUsersData: Stream[User] = {
    val t = System.currentTimeMillis
    Stream(
      User("1","Sunny",new java.sql.Timestamp(t)),
      User("2","Anurag",new java.sql.Timestamp(t))
    )
  }

  def writeData(path:String, data:Stream[User]):Unit = {
    ParquetWriter.write(path, data)
  }

}