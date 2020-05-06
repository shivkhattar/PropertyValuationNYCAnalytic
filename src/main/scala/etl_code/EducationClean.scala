/**
 * @author Rutvik Shah(rss638)
 */
package etl_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util_code.CommonUtil
import util_code.CommonConstants._

//  Service which reads education data and performs data cleaning
object EducationClean extends Clean {

  def clean(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit = {

    val data = sc.textFile(inputPath).filter(!_.startsWith(ED_FISCAL_YEAR))

    //  Filter entries to obtain entries of only defined length (remove ones with missing data points)
    //  Keep only the columns described in the data schema
    val columnsRemoved = data.map(_.split(SPLIT_REGEX))
      .filter(_.length == 39)
      .map(x => Map(ED_ATS_SYSTEM_CODE -> x(0), ED_BBL -> x(20), ED_LOCATION_NAME -> x(4), ED_GRADES_FINAL_TEXT -> x(9), ED_OPEN_DATE -> x(10), ED_LOCATION1 -> x(38)))

    //  Check for empty fields
    val cleanedEducation = columnsRemoved.filter(row => !row(ED_ATS_SYSTEM_CODE).isEmpty && !row(ED_BBL).isEmpty && !row(ED_LOCATION1).isEmpty)

    //  Create tuples for each entry and save the cleaned file
    val tupledEdu = cleanedEducation.map(row => (row(ED_ATS_SYSTEM_CODE).trim, row(ED_BBL).trim,
      getLatLong(row(ED_LOCATION1).trim, isLatitude = true), getLatLong(row(ED_LOCATION1), isLatitude = false).trim,
      getZipCode(row(ED_LOCATION1)).trim, row(ED_LOCATION_NAME).trim, row(ED_GRADES_FINAL_TEXT).trim, row(ED_OPEN_DATE).trim))
      .map(tup => tup.toString.substring(1, tup.toString.length - 1))

    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    tupledEdu.saveAsTextFile(outputPath)
  }

  // Helper function to extract latitude and longitude from the given address string from a data entry
  def getLatLong(addressString: String, isLatitude: Boolean): String = {
    val substr = addressString.substring(addressString.indexOf(ADDR_SUBSTRING_PARAM))
    val split = substr.substring(substr.indexOf(ED_LAT_LONG_PREFIX) + 1).replace(ED_LAT_LONG_SUFFIX, "")
    if (isLatitude) split.substring(0, split.indexOf(ED_LAT_LONG_SEPARATOR)).replace(ED_LAT_LONG_SEPARATOR, "")
    else split.substring(split.indexOf(ED_LAT_LONG_SEPARATOR) + 1)
  }

  // Helper function to extract zipcode from the given address string from a data entry
  def getZipCode(addressString: String): String = {
    val substr = addressString.substring(addressString.indexOf(ADDR_SUBSTRING_PARAM))
    val split = substr.substring(substr.indexOf(ADDR_SUBSTRING_PARAM) + 3, substr.indexOf(ED_LAT_LONG_PREFIX))
    split.trim
  }
}