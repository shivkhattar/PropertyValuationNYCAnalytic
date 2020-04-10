package profile

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonConstants.{ADDRESS, ADDRESS_LENGTH_RANGE_KEY, BBL, BOROUGH, PLUTO_PROFILE_PATHS, SPLIT_REGEX, ZIPCODE}
import util.CommonUtil
import util.CommonUtil.getCountsGroupedByKeyForField


object PlutoProfile extends Profile {
  def profile(sc: SparkContext, hdfs: FileSystem, originalInputPath: String, inputPath: String, outputPath: String): Unit = {
    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    CommonUtil.writeOriginalCount(sc, originalInputPath, outputPath)

    val data = sc.textFile(inputPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(BBL -> x(0), BOROUGH -> x(2), ZIPCODE -> x(5), ADDRESS -> x(6)))

    val count = CommonUtil.getTotalCount(data)
    count.saveAsTextFile(outputPath + PLUTO_PROFILE_PATHS(BBL))

    val boroughs = getCountsGroupedByKeyForField(data, BOROUGH)
    boroughs.saveAsTextFile(outputPath + PLUTO_PROFILE_PATHS(BOROUGH))

    val zipcodes = getCountsGroupedByKeyForField(data, ZIPCODE)
    zipcodes.saveAsTextFile(outputPath + PLUTO_PROFILE_PATHS(ZIPCODE))

    val nameLengthRange = CommonUtil.getLengthRange(data, ADDRESS, ADDRESS_LENGTH_RANGE_KEY)
    nameLengthRange.saveAsTextFile(outputPath + PLUTO_PROFILE_PATHS(ADDRESS))
  }
}