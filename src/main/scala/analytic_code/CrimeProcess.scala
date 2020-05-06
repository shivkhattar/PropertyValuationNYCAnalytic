/**
 * @author Shiv Khattar(sk8325)
 */

package analytic_code

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util_code.CommonConstants._

//  Service which reads cleaned crime data and performs processing
object CrimeProcess {

  //  Function that handles the processing of cleaned crime data
  def process(sc: SparkContext, cleanedCrimePath: String, processedPlutoRDD: RDD[Map[String, String]],
              processedPlutoData: List[Map[String, String]]): RDD[(String, (Double, (String, String)))] = {

    //  Read the data from the path and create a Map for the data that is needed for processing
    val crimeData = sc.textFile(cleanedCrimePath, 5)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(CMPLNT_NUM -> x(0), LEVEL -> x(3), LATITUDE -> x(5), LONGITUDE -> x(6)))

    //  Call the common processUtil service that deals with calculating scores from the data
    ProcessUtil.getScoresForData(sc, crimeData, processedPlutoRDD, processedPlutoData, getScoreForLevel)
  }

  //  Helper function that returns the score as 1 when the crime is a Felony and 0 otherwise
  def getScoreForLevel(level: String): Double = {
    if (level.equals(CRIME_LEVEL_FELONY)) 1 else 0
  }
}
