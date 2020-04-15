package profile

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonUtil
import util.CommonUtil.getCountsGroupedByKeyForField
import org.apache.spark.rdd.RDD
import util.EdPropConstants.{ED_ATS_SYSTEM_CODE, ED_LOCATION_NAME, ED_BBL, ED_GRADES_FINAL_TEXT, ED_OPEN_DATE, ED_LOCATION1
  , ED_LAT_LONG_PREFIX, ED_LAT_LONG_SUFFIX, ED_LAT_LONG_SEPARATOR, ED_LATITUDE, ED_LONGITUDE, ED_ZIP_CODE, SPLIT_REGEX, ED_PROFILE_PATHS, PROFILER_SEPARATOR}
import util.CommonUtil
import util.CommonUtil.getCountsGroupedByKeyForField

object EducationProfile extends Profile {

  def profile(sc: SparkContext, hdfs: FileSystem, originalInputPath: String, inputPath: String, outputPath: String): Unit = {
    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    CommonUtil.writeOriginalCount(sc, originalInputPath, outputPath)

    val data = sc.textFile(inputPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(ED_ATS_SYSTEM_CODE -> x(0), ED_BBL -> x(1), ED_LATITUDE -> x(2), ED_LONGITUDE -> x(3), ED_ZIP_CODE -> x(4)
        , ED_LOCATION_NAME -> x(5), ED_GRADES_FINAL_TEXT -> x(6), ED_OPEN_DATE -> x(7)))

    val count = CommonUtil.getTotalCount(data)
    count.saveAsTextFile(outputPath + ED_PROFILE_PATHS(ED_ATS_SYSTEM_CODE))

    val dates = getCountsGroupedByYearBuilt(data)
    dates.saveAsTextFile(outputPath + ED_PROFILE_PATHS(ED_OPEN_DATE))

    val boroughs = getCountsGroupedByKeyForField(data, ED_BBL)
    boroughs.saveAsTextFile(outputPath + ED_PROFILE_PATHS(ED_BBL))

    val offenses = getCountsGroupedByKeyForField(data, ED_ZIP_CODE)
    offenses.saveAsTextFile(outputPath + ED_PROFILE_PATHS(ED_ZIP_CODE))
  }

  private def getCountsGroupedByYearBuilt(data: RDD[Map[String, String]]) = {
    data.map(row => row(ED_OPEN_DATE).split("/")(2).trim.substring(0,4))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString())
  }
}
