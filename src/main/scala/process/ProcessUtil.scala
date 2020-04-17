package process

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{BOROUGH_BLOCK, LATITUDE, LEVEL, LONGITUDE, RADIUS_OF_EARTH_IN_KM, ZERO_SCORE, RANGE_IN_KM}

object ProcessUtil {

  def getScoresForData(sc: SparkContext, crimeData: RDD[Map[String, String]], totalCrimeScore: Double, processedPlutoData: List[Map[String, String]], getScore: String => Double): RDD[(String, (Double, (String, String)))] = {
    val crimeScoreRDD = crimeData.map(row => processedPlutoData
      .map(y => (y(BOROUGH_BLOCK), calculateDistance((row(LATITUDE), row(LONGITUDE)), (y(LATITUDE), y(LONGITUDE))), getScore(row(LEVEL).trim)))
      .minBy(_._2))
      .map(x => (x._1, if (inRange(x._2)) x._3 else ZERO_SCORE)).reduceByKey(_ + _).map(x => (x._1, (x._2.toDouble * 100) / totalCrimeScore))
    val plutoRDD = getRDDForPlutoData(sc, processedPlutoData)
    crimeScoreRDD.rightOuterJoin(plutoRDD).mapValues(option => if (option._1.isEmpty) (ZERO_SCORE, option._2) else (option._1.get, option._2))
  }

  private def inRange(distance: Double): Boolean = distance < RANGE_IN_KM

  // location = (latitude, longitude)
  private def calculateDistance(location1: (String, String), location2: (String, String)): Double = {
    val location1InRadians = (location1._1.toDouble.toRadians, location1._2.toDouble.toRadians)
    val location2InRadians = (location2._1.toDouble.toRadians, location2._2.toDouble.toRadians)
    val difference = (location2InRadians._1 - location1InRadians._1, location2InRadians._2 - location1InRadians._2)
    RADIUS_OF_EARTH_IN_KM * 2 * math.asin(math.sqrt(math.pow(math.sin(difference._1 / 2), 2)
      + math.cos(location1InRadians._1) * math.cos(location2InRadians._1) * math.pow(math.sin(difference._2 / 2), 2)))
  }

  private def getRDDForPlutoData(sc: SparkContext, processedPlutoData: List[Map[String, String]]) = {
    sc.parallelize(processedPlutoData).map(y => (y(BOROUGH_BLOCK), (y(LATITUDE), y(LONGITUDE))))
  }
}