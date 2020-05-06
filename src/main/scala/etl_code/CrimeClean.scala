/**
 * @author Shiv Khattar(sk8325)
 */

package etl_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util_code.CommonUtil
import util_code.CommonConstants.{CRIME_YEAR_FROM, COMMA, SPLIT_REGEX, BOROUGH, CMPLNT_NUM, DATE, LATITUDE, LEVEL, LONGITUDE, OFFENSE_DESC, SUSPECT_AGE, SUSPECT_RACE, SUSPECT_SEX, X_COORD, Y_COORD, UNKNOWN}

//  Service which reads crime data and performs data cleaning
object CrimeClean extends Clean {

  def clean(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit = {
    val data = sc.textFile(inputPath).filter(!_.startsWith(CMPLNT_NUM)).map(d => if (d.endsWith(COMMA)) d.concat(UNKNOWN) else d)

    //  Filter entries to obtain entries of only defined length (remove ones with missing data points)
    //  Keep only the columns described in the data schema
    val columnsRemoved = data.map(_.split(SPLIT_REGEX))
      .filter(_.length == 35)
      .map(x => Map(CMPLNT_NUM -> x(0), DATE -> x(6), OFFENSE_DESC -> x(8), LEVEL -> x(12), BOROUGH -> x(13),
        LATITUDE -> x(27), LONGITUDE -> x(28), X_COORD -> x(21), Y_COORD -> x(22), SUSPECT_AGE -> x(23), SUSPECT_RACE -> x(24), SUSPECT_SEX -> x(25)))

    //  Check for empty fields and filter based on the year
    val cleanedCrime = columnsRemoved.filter(row => !row(CMPLNT_NUM).isEmpty && !row(DATE).isEmpty && !row(LATITUDE).isEmpty && !row(LONGITUDE).isEmpty && !row(X_COORD).isEmpty && !row(Y_COORD).isEmpty)
      .filter(row => row(DATE).trim.substring(6).toInt >= CRIME_YEAR_FROM)

    //  Create tuples for each entry and save the cleaned file
    val tupledCrime = cleanedCrime.map(row => (row(CMPLNT_NUM), row(DATE), CommonUtil.updateValueIfBlank(row(OFFENSE_DESC)), CommonUtil.updateValueIfBlank(row(LEVEL)), CommonUtil.updateValueIfBlank(row(BOROUGH)),
      row(LATITUDE), row(LONGITUDE), row(X_COORD), row(Y_COORD), CommonUtil.updateValueIfBlank(getCleanedAgeRange(row(SUSPECT_AGE))), CommonUtil.updateValueIfBlank(row(SUSPECT_RACE)), CommonUtil.updateValueIfBlank(row(SUSPECT_SEX))))
      .map(tup => tup.toString.substring(1, tup.toString.length - 1))

    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    tupledCrime.saveAsTextFile(outputPath)
  }

  //  Helper function to clean the Age Range data for profiling
  def getCleanedAgeRange(age: String): String = {
    val ageRangeSet = Set("<18", "18-24", "25-44", "45-64", "65+")
    if (ageRangeSet.contains(age)) age else UNKNOWN
  }
}