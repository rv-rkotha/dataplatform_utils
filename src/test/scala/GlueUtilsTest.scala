import com.goibibo.dp.utils.GlueUtilsTypes._
import com.goibibo.dp.utils.{GlueUtils,GlueUtilsPrivate}
import org.scalatest._

class ExampleSpec extends FlatSpec with Matchers {

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
    val partitionKeys = Seq(FieldInfo("year","Int"))
    val fields        = Seq(FieldInfo("id","Int"))
    val location      = "s3://tmp-data-platform/test-sunny/"
    
    GlueUtils.createTable(tableDetails,partitionKeys, fields, location,ParquetFormat).get
    client.close
  }
}