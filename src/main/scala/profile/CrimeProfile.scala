package profile

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{SPLIT_REGEX, BOROUGH, CMPLNT_NUM, DATE, LATITUDE, LEVEL, LONGITUDE, OFFENSE_DESC, SUSPECT_AGE, SUSPECT_RACE, SUSPECT_SEX, X_COORD, Y_COORD, CRIME_PROFILE_PATHS, PROFILER_SEPARATOR, COUNT_KEY}
import util.CommonUtil
import util.CommonUtil.getCountsGroupedByKeyForField

object CrimeProfile extends Profile {

  def profile(sc: SparkContext, hdfs: FileSystem, originalInputPath: String, inputPath: String, outputPath: String): Unit = {
    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    CommonUtil.writeOriginalCount(sc, originalInputPath, outputPath)

    val data = sc.textFile(inputPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(CMPLNT_NUM -> x(0), DATE -> x(1), OFFENSE_DESC -> x(2), LEVEL -> x(3), BOROUGH -> x(4),
        LATITUDE -> x(5), LONGITUDE -> x(6), X_COORD -> x(7), Y_COORD -> x(8), SUSPECT_AGE -> x(9), SUSPECT_RACE -> x(10), SUSPECT_SEX -> x(11)))

    val count = CommonUtil.getTotalCount(data)
    count.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(CMPLNT_NUM))

    val dates = getCountsGroupedByYear(data)
    dates.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(DATE))

    val boroughs = getCountsGroupedByKeyForField(data, BOROUGH)
    boroughs.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(BOROUGH))

    val offenses = getCountsGroupedByKeyForField(data, OFFENSE_DESC)
    offenses.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(OFFENSE_DESC))

    val levels = getCountsGroupedByKeyForField(data, LEVEL)
    levels.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(LEVEL))

    val age = getCountsGroupedByKeyForField(data, SUSPECT_AGE)
    age.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(SUSPECT_AGE))

    val race = getCountsGroupedByKeyForField(data, SUSPECT_RACE)
    race.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(SUSPECT_RACE))

    val sex = getCountsGroupedByKeyForField(data, SUSPECT_SEX)
    sex.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(SUSPECT_SEX))
  }

  private def getCountsGroupedByYear(data: RDD[Map[String, String]]) = {
    data.map(row => row(DATE))
      .map(x => x.trim.substring(6))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString())
  }
}