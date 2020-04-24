package process

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{BOROUGH_BLOCK, LATITUDE, LEVEL, LONGITUDE, RADIUS_OF_EARTH_IN_KM, ZERO_SCORE, RANGE_IN_KM}

object ProcessUtil {

  def getScoresForData(sc: SparkContext, data: RDD[Map[String, String]], processedPlutoRDD: RDD[Map[String, String]],
                       processedPlutoData: List[Map[String, String]], getScore: String => Double): RDD[(String, (Double, (String, String)))] = {
    val scoreRDD = data.map(row => processedPlutoData
      .map(y => (y(BOROUGH_BLOCK), calculateDistance((row(LATITUDE), row(LONGITUDE)), (y(LATITUDE), y(LONGITUDE))), getScore(row(LEVEL).trim))))
      .flatMap(x => x.map(y => y))
      .map(x => (x._1, getScoreBasedOnDistance(x._2, x._3))).reduceByKey(_ + _)

    val max = scoreRDD.map(x => ("max", x._2)).reduceByKey(math.max(_, _)).map(_._2).collect()(0)
    val finalScoreRDD = scoreRDD.map(x => (x._1, x._2 * 10 / max))
    val plutoRDD = processedPlutoRDD.map(y => (y(BOROUGH_BLOCK), (y(LATITUDE), y(LONGITUDE))))
    finalScoreRDD.rightOuterJoin(plutoRDD).mapValues(option => if (option._1.isEmpty) (ZERO_SCORE, option._2) else (option._1.get, option._2))
  }

  // location = (latitude, longitude)
  private def calculateDistance(location1: (String, String), location2: (String, String)): Double = {
    val location1InRadians = (location1._1.toDouble.toRadians, location1._2.toDouble.toRadians)
    val location2InRadians = (location2._1.toDouble.toRadians, location2._2.toDouble.toRadians)
    val difference = (location2InRadians._1 - location1InRadians._1, location2InRadians._2 - location1InRadians._2)
    RADIUS_OF_EARTH_IN_KM * 2 * math.asin(math.sqrt(math.pow(math.sin(difference._1 / 2), 2)
      + math.cos(location1InRadians._1) * math.cos(location2InRadians._1) * math.pow(math.sin(difference._2 / 2), 2)))
  }

  private def getScoreBasedOnDistance(distance: Double, score: Double): Double = {
    if (distance <= RANGE_IN_KM / 3) score * 3.0
    else if (distance <= RANGE_IN_KM / 2) score * 2.0
    else if (distance <= RANGE_IN_KM) score
    else ZERO_SCORE
  }
}