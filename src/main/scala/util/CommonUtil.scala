package util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{COUNT_KEY, LINE, ORIGINAL_COUNT_PATH, PROFILER_SEPARATOR, SPLIT_REGEX, UNKNOWN}

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

  def getAvgLatLong(inputRDD: RDD[(String, Iterable[String])]) : RDD[(String, String)] = {
    val outputRDD = inputRDD.map(line => (line._1, line._2.to[collection.immutable.Seq].toList))
      .map(tup => (tup._1, getAvgFromList(tup._2)))
    return outputRDD
  }

  def getAvgFromList(inputList: List[String]): String ={
    var x : Double = 0.0
    var y : Double = 0.0
    var z : Double = 0.0
    for(s <- inputList) {
      val a = s.split(SPLIT_REGEX)
      val lat = a(1).toDouble
      val lon = a(2).toDouble
      x = x + math.cos(lat) * math.cos(lon)
      y = y + math.cos(lat) * math.sin(lon)
      z = z + math.sin(lat)
    }
    val length : Double = inputList.size.toDouble
    x = x / length
    y = y / length
    z = z / length
    val latLong : String = math.atan2(z, math.sqrt(x * x + y * y)).toString + ", " + math.atan2(y, x).toString
    return latLong
  }
}