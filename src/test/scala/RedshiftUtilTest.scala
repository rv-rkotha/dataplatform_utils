package com.goibibo.dp.utils

import org.scalatest._
import pureconfig.loadConfigOrThrow
import resource._

class RedshiftUtilTest extends FlatSpec with Matchers {

  import pureconfig.generic.auto._
  final case class RedshiftConf(  clusterId:String,user:String, dbGroup:String, autoCreate:String, region:String)
  val redshiftConf = loadConfigOrThrow[RedshiftConf]("redshift")
  import redshiftConf._

  implicit val client = RedshiftUtil.getClusterClient(region)

  "Redshift endpoint" should "work" in {
    val endpoint = RedshiftUtil.getClusterEndPointDetails(clusterId).get
    endpoint.dbName should be("goibibo")
  }

  "Redshift credentials" should "work with known-user" in {
    val credentials = RedshiftUtil.getTempCredentials(clusterId, user).get
    credentials.password.size should be >0
  }

  "Redshift connection" should "work" in {

    var success:Boolean = false
    for(
        connection <- managed(RedshiftUtil.getConnection(clusterId,user).get);
        rs <- RedshiftUtil.executeQuery(connection, "select 'hello' h, 'world' w")
    ) {
      rs.next()
      rs.getString("h") should be("hello")
      rs.getString("w") should be("world")
      success = true
    }
    success should be(true)
  }
}
