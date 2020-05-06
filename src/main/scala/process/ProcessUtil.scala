package process

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{BOROUGH_BLOCK, LATITUDE, LEVEL, LONGITUDE, RADIUS_OF_EARTH_IN_KM, ZERO_SCORE, RANGE_IN_KM}

object ProcessUtil {

  def getScoresForData(sc: SparkContext, data: RDD[Map[String, String]], processedPlutoRDD: RDD[Map[String, String]],
                       processedPlutoData: List[Map[String, String]], getScore: String => Double): RDD[(String, (Double, (String, String)))] = {
    val scoreRDD = data.map(row => (getScore(row(LEVEL)), processedPlutoData.filter(y => inRange(calculateDistance((row(LATITUDE), row(LONGITUDE)), (y(LATITUDE), y(LONGITUDE)))))))
      .flatMap(row => row._2.map(y => (y(BOROUGH_BLOCK), row._1)))
      .reduceByKey(_ + _)

    val total = scoreRDD.map(x => ("total", x._2)).reduceByKey(_ + _).collect()(0)._2

    val finalScoreRDD = scoreRDD.map(x => (x._1, x._2 / total))
    val max_min = finalScoreRDD.map(x => ("max_min", (x._2, x._2)))
      .reduceByKey((d1, d2) => (math.max(d1._1, d2._1), math.min(d1._2, d2._2))).map(_._2).collect()(0)
    val final2 = finalScoreRDD.map(x => (x._1, 10 * ((x._2 - max_min._2) / (max_min._1 - max_min._2))))
    val plutoRDD = processedPlutoRDD.map(y => (y(BOROUGH_BLOCK), (y(LATITUDE), y(LONGITUDE))))
    final2.rightOuterJoin(plutoRDD).mapValues(option => if (option._1.isEmpty) (ZERO_SCORE, option._2) else (option._1.get, option._2))
  }

  private def inRange(distance: Double) = distance <= RANGE_IN_KM

  // location = (latitude, longitude)
  private def calculateDistance(location1: (String, String), location2: (String, String)): Double = {
    val location1InRadians = (location1._1.toDouble.toRadians, location1._2.toDouble.toRadians)
    val location2InRadians = (location2._1.toDouble.toRadians, location2._2.toDouble.toRadians)
    val difference = (location2InRadians._1 - location1InRadians._1, location2InRadians._2 - location1InRadians._2)
    RADIUS_OF_EARTH_IN_KM * 2 * math.asin(math.sqrt(math.pow(math.sin(difference._1 / 2), 2)
      + math.cos(location1InRadians._1) * math.cos(location2InRadians._1) * math.pow(math.sin(difference._2 / 2), 2)))
  }
}