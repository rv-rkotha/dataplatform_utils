package com.goibibo.dp.utils
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.fs
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.net.URI

object S3Utils {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def deleteS3Data(location:String, conf:Configuration = new Configuration()): Unit = {
    logger.info("Deleting : " + location)
    // following line is just useful to detect the file system in use is s3a
    val s3BucketObject: FileSystem = FileSystem.get(new URI(location), conf)
    try {
      s3BucketObject.delete(new fs.Path(location), true)
    } catch {
      case e: Exception => {
        logger.error(s"Exception occurred while deleting s3 data $location " + e.getMessage)
        System.exit(1)
      }
    }
  }
}
