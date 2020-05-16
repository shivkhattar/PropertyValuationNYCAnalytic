/**
 * @author Rutvik Shah(rss638)
 */

package profiling_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util_code.CommonUtil
import util_code.CommonUtil.getCountsGroupedByKeyForField
import util_code.CommonConstants._

//  Service to handle profiling of Property data
object PropertyProfile extends Profile {

  //  Function to profile property data
  def profile(sc: SparkContext, hdfs: FileSystem, originalInputPath: String, inputPath: String, outputPath: String): Unit = {
    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)

    //  Total count of the data points before cleaning in the original dataset
    CommonUtil.writeOriginalCount(sc, originalInputPath, outputPath)

    //  Read the cleaned data and build a Map from the data
    val data = sc.textFile(inputPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(PROP_PARID -> x(0), PROP_ZIPCODE -> x(1), PROP_CURMKTTOT -> x(2), PROP_BORO -> x(3), PROP_BLOCK -> x(4)
        , PROP_LOT -> x(5), PROP_ZONING -> x(6), PROP_YRBUILT -> x(7)))

    //  Total count of property data entries remaining after cleaning.
    val count = CommonUtil.getTotalCount(data)
    count.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_PARID))

    //  Total count of schools built in each year
    val dates = getCountsGroupedByYearBuilt(data)
    dates.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_YRBUILT))

    //  Number of properties in each borough-block in the dataset
    val boroughs = getCountsGroupedByKeyForField(data, PROP_BORO)
    boroughs.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_BORO))

    //  Number of properties in each zoning area in the dataset
    val zoning = getCountsGroupedByKeyForField(data, PROP_ZONING)
    zoning.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_ZONING))

    //  Number of properties in each zip code in the dataset
    val levels = getCountsGroupedByKeyForField(data, PROP_ZIPCODE)
    levels.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_ZIPCODE))

    //  Get max and min property values in the entire dataset
    val max = getMaxPropertyVal(data)
    val min = getMinPropertyVal(data)
    val max_min = sc.parallelize(Seq(max.toString + " " + min.toString))
    max_min.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_MAX))
  }

  //  Helper function to calculate count of properties built in each year
  private def getCountsGroupedByYearBuilt(data: RDD[Map[String, String]]) = {
    data.map(row => row(PROP_YRBUILT))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString)
  }

  // Helper function to find the maximum property value from the given data
  private def getMaxPropertyVal(data: RDD[Map[String, String]]): Long = {
    data.map(row => row(PROP_CURMKTTOT).toLong).max
  }

  // Helper function to find the minimum property value from the given data
  private def getMinPropertyVal(data: RDD[Map[String, String]]): Long = {
    data.map(row => row(PROP_CURMKTTOT).toLong).min
  }
}