package profile

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonConstants._
import org.apache.spark.rdd.RDD
import util.CommonUtil
import util.CommonUtil.getCountsGroupedByKeyForField

object EducationProfile extends Profile {
  //  Function to profile education data
  def profile(sc: SparkContext, hdfs: FileSystem, originalInputPath: String, inputPath: String, outputPath: String): Unit = {
    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    CommonUtil.writeOriginalCount(sc, originalInputPath, outputPath)

    val data = sc.textFile(inputPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(ED_ATS_SYSTEM_CODE -> x(0), ED_BBL -> x(1), ED_LATITUDE -> x(2), ED_LONGITUDE -> x(3), ED_ZIP_CODE -> x(4)
        , ED_LOCATION_NAME -> x(5), ED_GRADES_FINAL_TEXT -> x(6), ED_OPEN_DATE -> x(7)))

    //  Total count of the data points remaining after cleaning
    val count = CommonUtil.getTotalCount(data)
    count.saveAsTextFile(outputPath + ED_PROFILE_PATHS(ED_ATS_SYSTEM_CODE))

    //  Total count of schools built in each year
    val dates = getCountsGroupedByYearBuilt(data)
    dates.saveAsTextFile(outputPath + ED_PROFILE_PATHS(ED_OPEN_DATE))

    //  Number of schools in each borough-block in the dataset
    val boroughs = getCountsGroupedByKeyForField(data, ED_BBL)
    boroughs.saveAsTextFile(outputPath + ED_PROFILE_PATHS(ED_BBL))

    //  Number of schools in each zipcode in the dataset
    val offenses = getCountsGroupedByKeyForField(data, ED_ZIP_CODE)
    offenses.saveAsTextFile(outputPath + ED_PROFILE_PATHS(ED_ZIP_CODE))
  }

  //  Helper function to calculate count of schools built in each year
  private def getCountsGroupedByYearBuilt(data: RDD[Map[String, String]]) = {
    data.map(row => row(ED_OPEN_DATE).split("/")(2).trim.substring(0,4))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString())
  }
}
