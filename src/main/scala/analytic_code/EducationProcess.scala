/**
 * @author Rutvik Shah(rss638)
 */

package analytic_code

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util_code.CommonConstants.{ED_ATS_SYSTEM_CODE, LATITUDE, LEVEL, LONGITUDE, SPLIT_REGEX, DEFAULT_LEVEL}

//  Service which reads cleaned education data and performs processing
object EducationProcess {

  //  Function that handles the processing of cleaned crime data
  def process(sc: SparkContext, cleanedEducationPath: String, processedPlutoRDD: RDD[Map[String, String]],
              processedPlutoData: List[Map[String, String]]): RDD[(String, (Double, (String, String)))] = {

    //  Read the data from the path and create a Map for the data that is needed for processing
    val educationData = sc.textFile(cleanedEducationPath).map(_.split(SPLIT_REGEX))
      .map(x => Map(ED_ATS_SYSTEM_CODE -> x(0), LATITUDE -> x(2), LONGITUDE -> x(3), LEVEL -> DEFAULT_LEVEL))

    //  Call the common processUtil service that deals with calculating scores from the data
    ProcessUtil.getScoresForData(sc, educationData, processedPlutoRDD, processedPlutoData, (level: String) => level.toDouble)
  }
}
