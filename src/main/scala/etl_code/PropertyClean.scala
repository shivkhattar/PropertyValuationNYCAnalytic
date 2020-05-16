/**
 * @author Rutvik Shah(rss638)
 */

package etl_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util_code.CommonUtil
import util_code.CommonConstants._

//  Service to read NYC property valuation data and performs data cleaning
object PropertyClean extends Clean {

  def clean(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit = {

    val data = sc.textFile(inputPath, 10)
      .filter(!_.startsWith(PROP_PARID)).map(d => if (d.endsWith(COMMA)) d.concat(UNKNOWN) else d)

    //  Filter entries to obtain valuations of only the year 2020.
    //  Keep only the columns described in the data schema.
    val columnsRemoved = data.map(_.split(SPLIT_REGEX))
      .filter(x => x.length == 139 && x(7).equals(PROP_FILTER_YEAR))
      .map(x => Map(PROP_PARID -> x(0), PROP_BORO -> x(1), PROP_BLOCK -> x(2), PROP_LOT -> x(3), PROP_EASEMENT -> x(4), PROP_ZIPCODE -> x(77), PROP_YRBUILT -> x(90), PROP_ZONING -> x(73),
        PROP_CURMKTTOT -> x(57), PROP_AREA -> x(88), PROP_ISRESIDENTIAL -> x(110), PROP_RESIDENTIAL_AREA -> x(124), PROP_CONSTRUCTED_AREA -> x(121)))

    //  Check for empty fields, remove entries with property value as '0', and with area '0'.
    val cleanedProperty = columnsRemoved.filter(row => !row(PROP_PARID).isEmpty && !row(PROP_ZIPCODE).isEmpty && !row(PROP_CURMKTTOT).isEmpty && !row(PROP_BORO).isEmpty
      && !row(PROP_BLOCK).isEmpty && !row(PROP_LOT).isEmpty && !row(PROP_CURMKTTOT).equals(PROP_ZERO) && !row(PROP_AREA).isEmpty && !row(PROP_AREA).equals(PROP_ZERO))

    //  Create tuples for each entry and save the cleaned file.
    val tupledProp = cleanedProperty.map(row => (row(PROP_PARID), row(PROP_ZIPCODE), row(PROP_CURMKTTOT), row(PROP_BORO), row(PROP_BLOCK), row(PROP_LOT), row(PROP_ZONING), row(PROP_YRBUILT), row(PROP_AREA)))
      .map(tup => tup.toString.substring(1, tup.toString.length - 1))

    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    tupledProp.saveAsTextFile(outputPath)
  }
}
