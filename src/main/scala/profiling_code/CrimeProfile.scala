/**
 * @author Shiv Khattar(sk8325)
 */

package profiling_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util_code.CommonConstants.{SPLIT_REGEX, BOROUGH, CMPLNT_NUM, DATE, LATITUDE, LEVEL, LONGITUDE, OFFENSE_DESC, SUSPECT_AGE, SUSPECT_RACE, SUSPECT_SEX, X_COORD, Y_COORD, CRIME_PROFILE_PATHS, PROFILER_SEPARATOR, COUNT_KEY}
import util_code.CommonUtil
import util_code.CommonUtil.getCountsGroupedByKeyForField

//  Service to handle profiling of Crime data
object CrimeProfile extends Profile {

  //  Function to profile Crime data
  def profile(sc: SparkContext, hdfs: FileSystem, originalInputPath: String, inputPath: String, outputPath: String): Unit = {
    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)

    //  Total count of the data points before cleaning in the original dataset
    CommonUtil.writeOriginalCount(sc, originalInputPath, outputPath)

    //  Read the cleaned data and build a Map from the data
    val data = sc.textFile(inputPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(CMPLNT_NUM -> x(0), DATE -> x(1), OFFENSE_DESC -> x(2), LEVEL -> x(3), BOROUGH -> x(4),
        LATITUDE -> x(5), LONGITUDE -> x(6), X_COORD -> x(7), Y_COORD -> x(8), SUSPECT_AGE -> x(9), SUSPECT_RACE -> x(10), SUSPECT_SEX -> x(11)))

    //  Total count of the data points remaining after cleaning
    val count = CommonUtil.getTotalCount(data)
    count.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(CMPLNT_NUM))

    //  Total count of the data points grouped by Year
    val dates = getCountsGroupedByYear(data)
    dates.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(DATE))

    //  Total count of the data points grouped by Borough
    val boroughs = getCountsGroupedByKeyForField(data, BOROUGH)
    boroughs.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(BOROUGH))

    //  Total count of the data points grouped by Offense Description
    val offenses = getCountsGroupedByKeyForField(data, OFFENSE_DESC)
    offenses.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(OFFENSE_DESC))

    //  Total count of the data points grouped by Level of offense
    val levels = getCountsGroupedByKeyForField(data, LEVEL)
    levels.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(LEVEL))

    //  Total count of the data points grouped by Suspect Age
    val age = getCountsGroupedByKeyForField(data, SUSPECT_AGE)
    age.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(SUSPECT_AGE))

    //  Total count of the data points grouped by Race of the suspect
    val race = getCountsGroupedByKeyForField(data, SUSPECT_RACE)
    race.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(SUSPECT_RACE))

    //  Total count of the data points grouped by sex of the suspect
    val sex = getCountsGroupedByKeyForField(data, SUSPECT_SEX)
    sex.saveAsTextFile(outputPath + CRIME_PROFILE_PATHS(SUSPECT_SEX))
  }

  //  Helper function that groups the data by the different years present in the data
  private def getCountsGroupedByYear(data: RDD[Map[String, String]]) = {
    data.map(row => row(DATE))
      .map(x => x.trim.substring(6))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString())
  }
}