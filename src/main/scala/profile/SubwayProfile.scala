package profile

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonUtil
import util.CommonConstants.{SPLIT_REGEX, OBJECT_ID, SUBWAY_LINE, LATITUDE, LONGITUDE, STATION_NAME, PROFILER_SEPARATOR, SUBWAY_LINE_SEPERATOR, SUBWAY_PROFILE_PATHS, DISTINCT_SUBWAY_LINES_KEY, NAME_LENGTH_RANGE_KEY, COUNT_KEY, DISTINCT_SUBWAY_LINES}

object SubwayProfile extends Profile {

  def profile(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit = {
    val data = sc.textFile(inputPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(OBJECT_ID -> x(0), STATION_NAME -> x(1), LATITUDE -> x(2), LONGITUDE -> x(2), SUBWAY_LINE -> x(4)))

    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)

    val count = getTotalCount(data)
    count.saveAsTextFile(outputPath + SUBWAY_PROFILE_PATHS(OBJECT_ID))

    val nameLengthRange = getNameLengthRange(data)
    nameLengthRange.saveAsTextFile(outputPath + SUBWAY_PROFILE_PATHS(STATION_NAME))

    val subwayLines = data.flatMap(row => row(SUBWAY_LINE).split(SUBWAY_LINE_SEPERATOR))

    val distinctSubwayLines = getDistinctSubwayLines(subwayLines)
    distinctSubwayLines.saveAsTextFile(outputPath + SUBWAY_PROFILE_PATHS(DISTINCT_SUBWAY_LINES))

    val countOfSubwayLines = getCountOfSubwayLines(subwayLines)
    countOfSubwayLines.saveAsTextFile(outputPath + SUBWAY_PROFILE_PATHS(SUBWAY_LINE))
  }

  private def getCountOfSubwayLines(subwayLines: RDD[String]) = {
    subwayLines.map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2)
  }

  private def getDistinctSubwayLines(subwayLines: RDD[String]) = {
    subwayLines
      .distinct
      .map(d => (DISTINCT_SUBWAY_LINES_KEY, 1))
      .reduceByKey(_ + _)
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2)
  }

  private def getNameLengthRange(data: RDD[Map[String, String]]) = {
    data.map(row => (NAME_LENGTH_RANGE_KEY, (row(STATION_NAME).length, row(STATION_NAME).length)))
      .reduceByKey((d1, d2) => (if (d1._1 < d2._1) d1._1 else d2._1, if (d1._2 > d2._2) d1._2 else d2._2))
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString())
  }

  private def getTotalCount(data: RDD[Map[String, String]]) = {
    data.map(d => (COUNT_KEY, 1))
      .reduceByKey(_ + _)
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2)
  }
}