package util

import org.apache.hadoop.fs.{FileSystem, Path}

object CommonUtil {

  val unknown = "UNKNOWN"

  def updateValueIfBlank(value: String): String = {
    if (isBlank(value)) unknown else value
  }

  def isBlank(value: String): Boolean = {
    value.isEmpty
  }

  def deleteFolderIfAlreadyExists(hdfs: FileSystem, outputPath: String): Unit =
    if (hdfs.exists(new Path(outputPath))) hdfs.delete(new Path(outputPath))
}
