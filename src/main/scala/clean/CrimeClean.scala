package clean

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonUtil
import util.CommonConstants.{SPLIT_REGEX, BOROUGH, CMPLNT_NUM, DATE, LATITUDE, LEVEL, LONGITUDE, OFFENSE_DESC, SUSPECT_AGE, SUSPECT_RACE, SUSPECT_SEX, X_COORD, Y_COORD, UNKNOWN}

object CrimeClean extends Clean {

  def clean(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit = {
    val data = sc.textFile(inputPath).filter(!_.startsWith(CMPLNT_NUM)).map(d => if (d.endsWith(",")) d.concat(UNKNOWN) else d)

    val columnsRemoved = data.map(_.split(SPLIT_REGEX))
      .filter(_.length == 35)
      .map(x => Map(CMPLNT_NUM -> x(0), DATE -> x(6), OFFENSE_DESC -> x(8), LEVEL -> x(12), BOROUGH -> x(13),
        LATITUDE -> x(27), LONGITUDE -> x(28), X_COORD -> x(21), Y_COORD -> x(22), SUSPECT_AGE -> x(23), SUSPECT_RACE -> x(24), SUSPECT_SEX -> x(25)))

    val cleanedCrime = columnsRemoved.filter(row => !row(CMPLNT_NUM).isEmpty && !row(DATE).isEmpty && !row(LATITUDE).isEmpty && !row(LONGITUDE).isEmpty && !row(X_COORD).isEmpty && !row(Y_COORD).isEmpty)
      .filter(row => row(DATE).trim.substring(6).toInt >= 2014)

    val tupledCrime = cleanedCrime.map(row => (row(CMPLNT_NUM), row(DATE), CommonUtil.updateValueIfBlank(row(OFFENSE_DESC)), CommonUtil.updateValueIfBlank(row(LEVEL)), CommonUtil.updateValueIfBlank(row(BOROUGH)),
      row(LATITUDE), row(LONGITUDE), row(X_COORD), row(Y_COORD), CommonUtil.updateValueIfBlank(getCleanedAgeRange(row(SUSPECT_AGE))), CommonUtil.updateValueIfBlank(row(SUSPECT_RACE)), CommonUtil.updateValueIfBlank(row(SUSPECT_SEX))))
      .map(tup => tup.toString.substring(1, tup.toString.length - 1))

    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    tupledCrime.saveAsTextFile(outputPath)
  }

  def getCleanedAgeRange(age: String): String = {
    if(Set("<18", "18-24", "25-44", "45-64", "65+").contains(age)) age else UNKNOWN
  }
}