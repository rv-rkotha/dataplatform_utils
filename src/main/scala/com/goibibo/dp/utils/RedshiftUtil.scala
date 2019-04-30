package com.goibibo.dp.utils
import java.sql.{Connection, DriverManager, ResultSet}

import software.amazon.awssdk.services.redshift.model.GetClusterCredentialsRequest
import software.amazon.awssdk.services.redshift.RedshiftClient
import java.util.{Collections, Properties}

import software.amazon.awssdk.regions.Region

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

object RedshiftUtil {

  case class Credentials(user:String, password:String)
  case class DBEndpoint(hostname:String, port:Int, dbName: String)
  case class DBConnection(cred:Credentials, hostname:String, port:Int, db:String)

  def getClusterClient(region:String):RedshiftClient = RedshiftClient.builder().region(Region.of(region)).build()

  def getClusterEndPointDetails(clusterId:String)
                               (implicit client:RedshiftClient):Try[DBEndpoint] = {
    val result = client.describeClusters()
    val clusters = result.clusters().asScala.toSeq.filter(_.clusterIdentifier() == clusterId)
    if(clusters.length > 0) {
      val port = clusters(0).endpoint().port
      val hostname = clusters(0).endpoint().address
      val dbName = clusters(0).dbName()
      Success(DBEndpoint(hostname, port, dbName))
    } else {
      Failure(new IllegalArgumentException("clusterId not available") )
    }
  }

  def getTempCredentials(clusterId:String,  dbUser:String,
                         group:Option[String] = None, autoCreate:Boolean = false)
                        (implicit client:RedshiftClient):Try[Credentials] = {
    Try {
      val userGroups = group.map(v => Collections.singletonList(v)).getOrElse(Collections.emptyList[String]())
      val endpoint = getClusterEndPointDetails(clusterId).get

      val getClusterCredentialsReq = GetClusterCredentialsRequest.
        builder().
        dbGroups(userGroups).
        autoCreate(autoCreate).
        clusterIdentifier(clusterId).
        dbName(endpoint.dbName).
        dbUser(dbUser).
        build
      val response = client.getClusterCredentials(getClusterCredentialsReq)
      Credentials(response.dbUser, response.dbPassword)
    }
  }

  def getConnection(clusterId:String, dbUser:String,
                    group:Option[String] = None, autoCreate:Boolean = false)
                   (implicit client:RedshiftClient) :Try[Connection] = {
    for {
      credentials <- getTempCredentials(clusterId,dbUser, group, autoCreate)
      endpoint    <- getClusterEndPointDetails(clusterId)
      connection <- getConnectionInternal(credentials, endpoint)
    } yield (connection)

  }

  def getConnectionInternal(credentials: Credentials, endpoint: DBEndpoint):Try[Connection] = {
    Try {
      val connectionProps = new Properties()
      connectionProps.put("user", credentials.user)
      connectionProps.put("password", credentials.password)
      val jdbcUrl = s"jdbc:redshift://${endpoint.hostname}:${endpoint.port}/${endpoint.dbName}?useSSL=false"
      Class.forName("com.amazon.redshift.jdbc4.Driver")
      DriverManager.getConnection(jdbcUrl, connectionProps)
    }
  }

  def executeQuery(connection:Connection, query:String):Try[ResultSet] = {
    Try {
      val statement = connection.createStatement()
      statement.executeQuery(query)
    }
  }

}