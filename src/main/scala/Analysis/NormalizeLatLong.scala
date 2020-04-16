package Analysis

import util.CommonUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{BOROBLOCK, ADDRESS, BBL, BLOCK, BOROCODE, BOROUGH, BOROUGH_MAP, LATITUDE, LONGITUDE, LOT, SPLIT_REGEX, X_COORD, Y_COORD, ZIPCODE}
import util.CommonUtil.updateValueIfBlank

object NormalizeLatLong {
  def normalizeLatLongFromRDD(sc: SparkContext, hdfs: FileSystem, plutoOutputPath: String): Unit = {
    val inputRDD = sc.textFile(plutoOutputPath).map(_.split(SPLIT_REGEX))
      .map(x => (x(1).toString + " " + x(3).toString, x(5).toString + ", " + x(7).toString + ", " + x(8).toString))
      .groupByKey()
    val latLongPlutoData = CommonUtil.getAvgLatLong(inputRDD)
    val ans = latLongPlutoData.collect()
  }
}
