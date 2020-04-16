package util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{COUNT_KEY, LINE, ORIGINAL_COUNT_PATH, PROFILER_SEPARATOR, SPLIT_REGEX, UNKNOWN}
import util.CommonConstants.{COUNT_KEY, LINE, PROFILER_SEPARATOR, UNKNOWN, ORIGINAL_COUNT_PATH, RADIUS_OF_EARTH_IN_KM}

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

  def getLengthRange(data: RDD[Map[String, String]], field: String, key: String) = {
    data.map(row => (key, (row(field).length, row(field).length)))
      .reduceByKey((d1, d2) => (if (d1._1 < d2._1) d1._1 else d2._1, if (d1._2 > d2._2) d1._2 else d2._2))
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString())
  }

  def writeOriginalCount(sc: SparkContext, originalInputPath: String, outputPath: String) = {
    val originalData = sc.textFile(originalInputPath).map(x => Map(LINE -> x));
    val originalCount = getTotalCount(originalData);
    originalCount.saveAsTextFile(outputPath + ORIGINAL_COUNT_PATH)
  }

  def inRange(distance: Double): Boolean = distance < 100

  def inRange(location1: (String, String), location2: (String, String)): Boolean = calculateDistance(location1, location2) < 10


  // location = (latitude, longitude)
  def calculateDistance(location1: (String, String), location2: (String, String)): Double = {
    val location1InRadians = (location1._1.toDouble.toRadians, location1._2.toDouble.toRadians)
    val location2InRadians = (location2._1.toDouble.toRadians, location2._2.toDouble.toRadians)
    val difference = (location2InRadians._1 - location1InRadians._1, location2InRadians._2 - location1InRadians._2)
    val result = RADIUS_OF_EARTH_IN_KM * 2 * math.asin(math.sqrt(math.pow(math.sin(difference._1 / 2), 2)
      + math.cos(location1InRadians._1) * math.cos(location2InRadians._1) * math.pow(math.sin(difference._2 / 2), 2)))
    result
  }
}