package process

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{BOROUGH_BLOCK, LATITUDE, LONGITUDE, SPLIT_REGEX}

object PlutoProcess {
  def normalizeLatLongFromRDD(sc: SparkContext, cleanedPlutoFilesPath: String): List[Map[String, String]] = {
    val inputRDD = sc.textFile(cleanedPlutoFilesPath).map(_.split(SPLIT_REGEX))
      .map(x => (x(1) + "_" + x(3), (x(7), x(8))))
      .groupByKey()
    val latLongPlutoData = getAvgLatLong(inputRDD).map(x => Map(BOROUGH_BLOCK -> x._1, LATITUDE -> x._2._1, LONGITUDE -> x._2._2))
    latLongPlutoData.collect().toList
  }

  private def getAvgLatLong(inputRDD: RDD[(String, Iterable[(String, String)])]): RDD[(String, (String, String))] = {
    inputRDD.map(line => (line._1, line._2.to[Seq].toList))
      .map(tup => (tup._1, getAvgLatLongFromList(tup._2)))
  }

  private def getAvgLatLongFromList(inputList: List[(String, String)]): (String, String) = {
    var x: Double = 0.0
    var y: Double = 0.0
    var z: Double = 0.0
    for (s <- inputList) {
      val lat = s._1.toDouble.toRadians
      val lon = s._2.toDouble.toRadians
      x = x + math.cos(lat) * math.cos(lon)
      y = y + math.cos(lat) * math.sin(lon)
      z = z + math.sin(lat)
    }
    val length: Double = inputList.length.toDouble
    x = x / length
    y = y / length
    z = z / length
    (math.atan2(z, math.sqrt(x * x + y * y)).toDegrees.toString, math.atan2(y, x).toDegrees.toString)
  }
}