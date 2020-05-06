/**
 * @author Shiv Khattar(sk8325)
 */

package analytic_code

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util_code.CommonConstants._

//  Service which reads cleaned subway data and performs processing
object SubwayProcess {

  //  Function that handles the processing of cleaned subway data
  def process(sc: SparkContext, cleanedSubwayPath: String, processedPlutoRDD: RDD[Map[String, String]],
              processedPlutoData: List[Map[String, String]]): RDD[(String, (Double, (String, String)))] = {

    //  Read the data from the path and create a Map for the data that is needed for processing
    val subwayData = sc.textFile(cleanedSubwayPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(OBJECT_ID -> x(0), LATITUDE -> x(2), LONGITUDE -> x(3), LEVEL -> (x(4).split("-").length + 1).toString))

    //  Call the common processUtil service that deals with calculating scores from the data
    ProcessUtil.getScoresForData(sc, subwayData, processedPlutoRDD, processedPlutoData, (level: String) => level.toDouble)
  }
}