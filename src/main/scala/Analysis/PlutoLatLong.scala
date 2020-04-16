package Analysis

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{BOROBLOCK, ADDRESS, BBL, BLOCK, BOROCODE, BOROUGH, BOROUGH_MAP, LATITUDE, LONGITUDE, LOT, SPLIT_REGEX, X_COORD, Y_COORD, ZIPCODE}
import util.CommonUtil
import util.CommonUtil.updateValueIfBlank

object PlutoLatLong {
  def getLatLong(sc: SparkContext, hdfs: FileSystem, plutoOutputPath: String): Unit = {
    val inputRDD = sc.textFile(plutoOutputPath).map(_.split(SPLIT_REGEX))
      .map(x => (x(1).toString + " " + x(3).toString, x(5).toString + ", " + x(7).toString + ", " + x(8).toString))
      .groupByKey()
    val LatLongPlutoData = getAvgLatLong(inputRDD)
    val ans = LatLongPlutoData.take(5).foreach(println)
  }

  def getAvgLatLong(inputRDD: RDD[(String, Iterable[String])]) : RDD[(String, String)] = {
    val outputRDD = inputRDD.map(line => (line._1, line._2.to[collection.immutable.Seq].toList))
      .map(tup => (tup._1, getAvgFromList(tup._2)))
    return outputRDD
  }
  def getAvgFromList(inputList: List[String]): String ={
    var x : Double = 0.0
    var y : Double = 0.0
    var z : Double = 0.0
    for(s <- inputList) {
      val a = s.split(SPLIT_REGEX)
      val lat = a(1).toDouble
      val lon = a(2).toDouble
      x = x + math.cos(lat) * math.cos(lon)
      y = y + math.cos(lat) * math.sin(lon)
      z = z + math.sin(lat)
    }
    val length : Double = inputList.size.toDouble
    x = x / length
    y = y / length
    z = z / length
    val latLong : String = math.atan2(z, math.sqrt(x * x + y * y)).toString + ", " + math.atan2(y, x).toString
    return latLong
  }
}
