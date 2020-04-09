package util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import util.CommonConstants.{COUNT_KEY, UNKNOWN, PROFILER_SEPARATOR}

object CommonUtil {

  def updateValueIfBlank(value: String): String = {
    if (isBlank(value)) UNKNOWN else value
  }

  def isBlank(value: String): Boolean = {
    value.isEmpty
  }

  def deleteFolderIfAlreadyExists(hdfs: FileSystem, outputPath: String): Unit =
    if (hdfs.exists(new Path(outputPath))) hdfs.delete(new Path(outputPath))

  def getTotalCount(data: RDD[Map[String, String]]) = {
    data.map(d => (COUNT_KEY, 1))
      .reduceByKey(_ + _)
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2)
  }

  def getCountsGroupedByKeyForField(data: RDD[Map[String, String]], field: String): RDD[String] = {
    data.map(row => row(field))
      .map(x => x.replace("\"", ""))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString())
  }

  def getLengthRange(data: RDD[Map[String, String]], field : String, key : String) = {
    data.map(row => (key, (row(field).length, row(field).length)))
      .reduceByKey((d1, d2) => (if (d1._1 < d2._1) d1._1 else d2._1, if (d1._2 > d2._2) d1._2 else d2._2))
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString())
  }
}