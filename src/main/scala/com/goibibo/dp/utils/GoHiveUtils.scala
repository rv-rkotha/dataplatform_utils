package org.apache.spark.sql.hive

import scala.util.{Try, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

object GoHiveUtils {
  def getMSC(spark: SparkSession): Try[org.apache.hadoop.hive.metastore.IMetaStoreClient] = Try {
    val hiveClientImpl = spark.sharedState
      .externalCatalog
      .unwrapped
      .asInstanceOf[org.apache.spark.sql.hive.HiveExternalCatalog].client
    val f = hiveClientImpl
      .getClass
      .getDeclaredMethod("org$apache$spark$sql$hive$client$HiveClientImpl$$client")
    f.setAccessible(true)
    val msc = f.invoke(hiveClientImpl)
      .asInstanceOf[org.apache.hadoop.hive.ql.metadata.Hive]
      .getMSC()
    msc
  } recoverWith {
    case t: Throwable => t.printStackTrace; Failure(t)
  }

  def toHiveTable(spark: SparkSession, ct: CatalogTable) = Try {
    val hiveClientImpl = spark.sharedState
      .externalCatalog
      .unwrapped
      .asInstanceOf[org.apache.spark.sql.hive.HiveExternalCatalog].client
    val toHiveTableMethod = hiveClientImpl
      .getClass
      .getMethod("toHiveTable",
                 Seq(Class.forName("org.apache.spark.sql.catalyst.catalog.CatalogTable"),
                     Class.forName("scala.Option")):_*
      )
    val table = toHiveTableMethod
      .invoke(null, ct, None)
      .asInstanceOf[org.apache.hadoop.hive.ql.metadata.Table]
    table
  } recoverWith {
    case t: Throwable => t.printStackTrace; Failure(t)
  }
}


