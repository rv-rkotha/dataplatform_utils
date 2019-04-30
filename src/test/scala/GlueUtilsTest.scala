package com.goibibo.dp.utils
import GlueUtilsTypes._
import org.scalatest._

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter}


class GlueUtilsTest extends FlatSpec with Matchers {

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

  "creation of Table" should "be successful" in {
    implicit val client = GlueUtils.createGlueClient()
    // tableDetails:TableDetails, partitionKeys: Fields,
    //  fields:Fields, location:String, fileFormat: GlueFileFormat
    val tableDetails  = TableDetails("testsunny","goibibo_e")
//    val partitionKeys = Seq(FieldInfo("year","Int"))
    val partitionKeys = Seq[FieldInfo]()
    val fields        = Seq(
                          FieldInfo("id","string"),
                          FieldInfo("name","string"),
                          FieldInfo("created","timestamp")
                        )
    val location      = "s3://tmp-data-platform/test-sunny/"

    GlueUtils.createTable(tableDetails,partitionKeys, fields, location,ParquetFormat).get
    client.close
  }

  "Read and Write to/From S3 of parquet file" should "successful" in {
    case class User(id: String, name: String, created: java.sql.Timestamp)

    val temp: Stream[User] = Stream(
      User("1","Sunny",new java.sql.Timestamp(System.currentTimeMillis)),
      User("2","Anurag",new java.sql.Timestamp(System.currentTimeMillis))
    )
    val users: Stream[User] = Stream[User]()
    (1 to 10000).foreach( (i) =>
      users ++: temp
    )
    val path = "s3a://tmp-data-platform/test-sunny/kjsdf.parquet"
    ParquetWriter.write(path, users)
//    ParquetReader.read[User](path).foreach(println)
  }

}