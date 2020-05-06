/**
 * @author Shiv Khattar(sk8325)
 */

package etl_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util_code.CommonConstants.{ADDRESS, BBL, BLOCK, BOROCODE, BOROUGH, BOROUGH_MAP, LATITUDE, LONGITUDE, LOT, SPLIT_REGEX, X_COORD, Y_COORD, ZIPCODE}
import util_code.CommonUtil
import util_code.CommonUtil.updateValueIfBlank

//  Service which reads pluto data and performs data cleaning
object PlutoClean extends Clean {

  def clean(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit = {
    val data = sc.textFile(inputPath).filter(!_.startsWith(BOROUGH.toLowerCase))

    //  Keep only the columns described in the data schema
    val columnsRemoved = data.map(_.split(SPLIT_REGEX))
      .map(x => Map(BOROCODE -> x(67), BOROUGH -> x(0), BLOCK -> x(1), LOT -> x(2), BBL -> x(68), ZIPCODE -> x(8), ADDRESS -> x(14),
        X_COORD -> x(71), Y_COORD -> x(72), LATITUDE -> x(73), LONGITUDE -> x(74)))

    //  Check for empty fields
    //  Create tuples for each entry and save the cleaned file
    val cleanedPluto = columnsRemoved.filter(row => !row(LATITUDE).isEmpty && !row(LONGITUDE).isEmpty)
      .map(row => (row(BBL), updateValueIfBlank(row(BOROCODE)), getBoroughFromCode(row(BOROUGH)), updateValueIfBlank(row(BLOCK)),
        updateValueIfBlank(row(LOT)), updateValueIfBlank(row(ZIPCODE)), updateValueIfBlank(row(ADDRESS)), row(LATITUDE), row(LONGITUDE), row(X_COORD), row(Y_COORD)))
      .map(tup => tup.toString.substring(1, tup.toString.length - 1))

    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    cleanedPluto.saveAsTextFile(outputPath)
  }

  def getBoroughFromCode(borough: String): String = BOROUGH_MAP(borough)
}