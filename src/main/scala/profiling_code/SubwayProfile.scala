/**
 * @author Shiv Khattar(sk8325)
 */

package profiling_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util_code.CommonUtil
import util_code.CommonConstants.{LINE, SPLIT_REGEX, OBJECT_ID, SUBWAY_LINE, LATITUDE, LONGITUDE, STATION_NAME, PROFILER_SEPARATOR, SUBWAY_LINE_SEPERATOR, SUBWAY_PROFILE_PATHS, DISTINCT_SUBWAY_LINES_KEY, NAME_LENGTH_RANGE_KEY, COUNT_KEY, DISTINCT_SUBWAY_LINES, ORIGINAL_COUNT_PATH}

//  Service to handle profiling of Subway data
object SubwayProfile extends Profile {

  //  Function to profile Subway data
  def profile(sc: SparkContext, hdfs: FileSystem, originalInputPath: String, cleanedInputPath: String, outputPath: String): Unit = {
    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)

    //  Total count of the data points before cleaning in the original dataset
    CommonUtil.writeOriginalCount(sc, originalInputPath, outputPath)

    //  Read the cleaned data and build a Map from the data
    val data = sc.textFile(cleanedInputPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(OBJECT_ID -> x(0), STATION_NAME -> x(1), LATITUDE -> x(2), LONGITUDE -> x(2), SUBWAY_LINE -> x(4)))

    //  Total count of the data points remaining after cleaning
    val count = CommonUtil.getTotalCount(data)
    count.saveAsTextFile(outputPath + SUBWAY_PROFILE_PATHS(OBJECT_ID))

    //  Get the range of the field Station Name
    val nameLengthRange = CommonUtil.getLengthRange(data, STATION_NAME, NAME_LENGTH_RANGE_KEY)
    nameLengthRange.saveAsTextFile(outputPath + SUBWAY_PROFILE_PATHS(STATION_NAME))

    val subwayLines = data.flatMap(row => row(SUBWAY_LINE).split(SUBWAY_LINE_SEPERATOR))

    //  Get Distinct subway lines
    val distinctSubwayLines = getDistinctSubwayLines(subwayLines)
    distinctSubwayLines.saveAsTextFile(outputPath + SUBWAY_PROFILE_PATHS(DISTINCT_SUBWAY_LINES))

    //  Total count of distinct subway lines
    val countOfSubwayLines = getCountOfSubwayLines(subwayLines)
    countOfSubwayLines.saveAsTextFile(outputPath + SUBWAY_PROFILE_PATHS(SUBWAY_LINE))
  }

  //  Helper function that returns the count of subway lines
  private def getCountOfSubwayLines(subwayLines: RDD[String]) = {
    subwayLines.map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2)
  }

  //  Helper function that returns distinct subway lines
  private def getDistinctSubwayLines(subwayLines: RDD[String]) = {
    subwayLines
      .distinct
      .map(d => (DISTINCT_SUBWAY_LINES_KEY, 1))
      .reduceByKey(_ + _)
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2)
  }
}