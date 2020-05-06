/**
 * @author Shiv Khattar(sk8325)
 */

package analytic_code

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util_code.CommonConstants.{BOROUGH_BLOCK, LATITUDE, LEVEL, LONGITUDE, RADIUS_OF_EARTH_IN_KM, ZERO_SCORE, RANGE_IN_KM, PROCESS_TOTAL, PROCESS_MAX_MIN}

//  Common utility service which generates scores for all data sets
object ProcessUtil {

  //  Function that generates scores for the dataset
  def getScoresForData(sc: SparkContext, data: RDD[Map[String, String]], processedPlutoRDD: RDD[Map[String, String]],
                       processedPlutoData: List[Map[String, String]], getScore: String => Double): RDD[(String, (Double, (String, String)))] = {

    //  Read the data and find the borough-block which is in range of each record in the data
    //  Add the score to that borough-block
    //  Flatmap all the borough-block and score records
    //  Reduce to add scores of each borough-block
    val scoreRDD = data.map(row => (getScore(row(LEVEL)), processedPlutoData.filter(y => inRange(calculateDistance((row(LATITUDE), row(LONGITUDE)), (y(LATITUDE), y(LONGITUDE)))))))
      .flatMap(row => row._2.map(y => (y(BOROUGH_BLOCK), row._1)))
      .reduceByKey(_ + _)

    //  Find the total score
    val total = scoreRDD.map(x => (PROCESS_TOTAL, x._2)).reduceByKey(_ + _).collect()(0)._2

    //  Find the final score for each borough-block by dividing it by the total scores across all borough-blocks
    val finalScoreRDD = scoreRDD.map(x => (x._1, x._2 / total))

    //  Find the maximum and minimum score across borough-blocks
    val max_min = finalScoreRDD.map(x => (PROCESS_MAX_MIN, (x._2, x._2)))
      .reduceByKey((d1, d2) => (math.max(d1._1, d2._1), math.min(d1._2, d2._2))).map(_._2).collect()(0)

    //  Normalize the scores by subtracting each score by the minimum score and dividing by the difference between maximumScore and minimum score
    //  Multiple by 10 to bring the scores on a scale of 10
    val final2 = finalScoreRDD.map(x => (x._1, 10 * ((x._2 - max_min._2) / (max_min._1 - max_min._2))))

    //  Get the processed pluto RDD with all borough-block and their latitude-longitude pairs
    val plutoRDD = processedPlutoRDD.map(y => (y(BOROUGH_BLOCK), (y(LATITUDE), y(LONGITUDE))))

    //Join the final scores with all borough-blocks and assign a ZERO score for each borough-block that is not found
    final2.rightOuterJoin(plutoRDD).mapValues(option => if (option._1.isEmpty) (ZERO_SCORE, option._2) else (option._1.get, option._2))
  }

  //  Helper function that returns a true if the distance is in range of 1.5kms
  private def inRange(distance: Double) = distance <= RANGE_IN_KM

  //  Calculate the difference between 2 (latitude, longitude) pairs
  //  Using Haversine Formula
  private def calculateDistance(location1: (String, String), location2: (String, String)): Double = {
    val location1InRadians = (location1._1.toDouble.toRadians, location1._2.toDouble.toRadians)
    val location2InRadians = (location2._1.toDouble.toRadians, location2._2.toDouble.toRadians)
    val difference = (location2InRadians._1 - location1InRadians._1, location2InRadians._2 - location1InRadians._2)
    RADIUS_OF_EARTH_IN_KM * 2 * math.asin(math.sqrt(math.pow(math.sin(difference._1 / 2), 2)
      + math.cos(location1InRadians._1) * math.cos(location2InRadians._1) * math.pow(math.sin(difference._2 / 2), 2)))
  }
}