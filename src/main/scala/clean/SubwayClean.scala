package clean

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonUtil._
import util.CommonConstants.{SPLIT_REGEX, OBJECT_ID, SUBWAY_LINE, LAT_LONG, STATION_NAME, URL, LAT_LONG_PREFIX, LAT_LONG_SUFFIX, LAT_LONG_SEPARATOR}

object SubwayClean extends Clean {

  def clean(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String) = {
    val data = sc.textFile(inputPath).filter(!_.startsWith(URL))
    val rowsRemoved = data.map(_.split(SPLIT_REGEX))
      .filter(_.length == 6)
      .map(x => Map(OBJECT_ID -> x(1), STATION_NAME -> x(2), LAT_LONG -> x(3), SUBWAY_LINE -> x(4)))

    val cleanedSubway = rowsRemoved.filter(row => !row(OBJECT_ID).isEmpty && !row(STATION_NAME).isEmpty && !row(LAT_LONG).isEmpty)
      .map(row => (row(OBJECT_ID), row(STATION_NAME), getSplitValue(row(LAT_LONG), true), getSplitValue(row(LAT_LONG), false), updateValueIfBlank(row(SUBWAY_LINE))))
      .map(tup => tup.toString.substring(1, tup.toString.length - 1))

    deleteFolderIfAlreadyExists(hdfs, outputPath)
    cleanedSubway.saveAsTextFile(outputPath)
  }

  def getSplitValue(latLong: String, isLatitude: Boolean): String = {
    val split = latLong.replace(LAT_LONG_PREFIX, "").replace(LAT_LONG_SUFFIX, "")
    if (isLatitude) split.substring(0, split.indexOf(LAT_LONG_SEPARATOR) + 1).replace(LAT_LONG_SEPARATOR, "")
    else split.substring(split.indexOf(LAT_LONG_SEPARATOR) + 1)
  }
}