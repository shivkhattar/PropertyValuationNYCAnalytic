/**
 * @author Shiv Khattar(sk8325)
 * @author Rutvik Shah(rss638)
 */

package util_code

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util_code.CommonConstants.{BOROUGH_BLOCK, COUNT_KEY, LATITUDE, LEVEL, LINE, LONGITUDE, ORIGINAL_COUNT_PATH, PROFILER_SEPARATOR, RADIUS_OF_EARTH_IN_KM, RANGE_IN_KM, UNKNOWN, ZERO_SCORE}

//  Common Helper object being called from more than 1 object
//  Reduces code duplication
object CommonUtil {

  //  Helper function to replace empty values with UNKNOWN
  def updateValueIfBlank(value: String): String = {
    if (isBlank(value)) UNKNOWN else value
  }

  //  Helper function that returns a true if the value is empty
  def isBlank(value: String): Boolean = {
    value.isEmpty
  }

  //  Helper function to delete a folder in HDFS if it already exists
  def deleteFolderIfAlreadyExists(hdfs: FileSystem, outputPath: String): Unit =
    if (hdfs.exists(new Path(outputPath))) hdfs.delete(new Path(outputPath))

  //  Helper founction to get the total count of rows in an RDD
  def getTotalCount(data: RDD[Map[String, String]]) = {
    data.map(d => (COUNT_KEY, 1))
      .reduceByKey(_ + _)
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2)
  }

  //  Helper function that groups the data by the parameter provided in the function
  def getCountsGroupedByKeyForField(data: RDD[Map[String, String]], field: String): RDD[String] = {
    data.map(row => row(field))
      .map(x => x.replace("\"", ""))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString())
  }

  //  Helper function tht gets the range of the length of the field provided in the function
  def getLengthRange(data: RDD[Map[String, String]], field: String, key: String) = {
    data.map(row => (key, (row(field).length, row(field).length)))
      .reduceByKey((d1, d2) => (if (d1._1 < d2._1) d1._1 else d2._1, if (d1._2 > d2._2) d1._2 else d2._2))
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString())
  }

  //  Total count of the data points before cleaning in the original dataset
  def writeOriginalCount(sc: SparkContext, originalInputPath: String, outputPath: String) = {
    val originalData = sc.textFile(originalInputPath).map(x => Map(LINE -> x));
    val originalCount = getTotalCount(originalData);
    originalCount.saveAsTextFile(outputPath + ORIGINAL_COUNT_PATH)
  }
}