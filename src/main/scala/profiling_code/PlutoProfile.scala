/**
 * @author Rutvik Shah(rss638)
 */

package profiling_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util_code.CommonConstants.{ADDRESS, ADDRESS_LENGTH_RANGE_KEY, BBL, BOROUGH, PLUTO_PROFILE_PATHS, SPLIT_REGEX, ZIPCODE}
import util_code.CommonUtil
import util_code.CommonUtil.getCountsGroupedByKeyForField

//  Service to handle profiling of Pluto data
object PlutoProfile extends Profile {

  //  Function to profile pluto data
  def profile(sc: SparkContext, hdfs: FileSystem, originalInputPath: String, inputPath: String, outputPath: String): Unit = {
    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)

    //  Total count of the data points before cleaning in the original dataset
    CommonUtil.writeOriginalCount(sc, originalInputPath, outputPath)

    //  Read the cleaned data and build a Map from the data
    val data = sc.textFile(inputPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(BBL -> x(0), BOROUGH -> x(2), ZIPCODE -> x(5), ADDRESS -> x(6)))

    //  Total count of the data points remaining after cleaning
    val count = CommonUtil.getTotalCount(data)
    count.saveAsTextFile(outputPath + PLUTO_PROFILE_PATHS(BBL))

    //  Total count of the data points grouped by Borough
    val boroughs = getCountsGroupedByKeyForField(data, BOROUGH)
    boroughs.saveAsTextFile(outputPath + PLUTO_PROFILE_PATHS(BOROUGH))

    //  Total count of the data points grouped by Zipcode
    val zipcodes = getCountsGroupedByKeyForField(data, ZIPCODE)
    zipcodes.saveAsTextFile(outputPath + PLUTO_PROFILE_PATHS(ZIPCODE))

    //  Get the range of the field Address
    val nameLengthRange = CommonUtil.getLengthRange(data, ADDRESS, ADDRESS_LENGTH_RANGE_KEY)
    nameLengthRange.saveAsTextFile(outputPath + PLUTO_PROFILE_PATHS(ADDRESS))
  }
}