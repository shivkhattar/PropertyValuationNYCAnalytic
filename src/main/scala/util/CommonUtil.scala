package util

import org.apache.hadoop.fs.{FileSystem, Path}
import util.CommonConstants.UNKNOWN

object CommonUtil {

  def updateValueIfBlank(value: String): String = {
    if (isBlank(value)) UNKNOWN else value
  }

  def isBlank(value: String): Boolean = {
    value.isEmpty
  }

  def deleteFolderIfAlreadyExists(hdfs: FileSystem, outputPath: String): Unit =
    if (hdfs.exists(new Path(outputPath))) hdfs.delete(new Path(outputPath))
}
