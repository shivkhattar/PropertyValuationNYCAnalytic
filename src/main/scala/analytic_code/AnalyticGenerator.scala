/**
 * @author Shiv Khattar(sk8325)
 */

package analytic_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util_code.CommonConstants.{PROCESSED_BB_DATA_PATH, PROCESSED_ZIPCODE_DATA_PATH, SPLIT_REGEX, ZIPCODE_POPULATION_PATH}
import util_code.CommonUtil.deleteFolderIfAlreadyExists

//  Service that handles joining all scores and generating final analytic
object AnalyticGenerator {

  //  Function that generates the final analytic
  def generateAnalytic(sc: SparkContext, hdfs: FileSystem, path: String, cleanedPlutoPath: String, subwayRDD: RDD[(String, (Double, (String, String)))],
                       crimeRDD: RDD[(String, (Double, (String, String)))], educationRDD: RDD[(String, (Double, (String, String)))], propertyRDD: RDD[(String, Double)]) = {

    //  Call the method that joins all scores per borough-block
    var now = System.currentTimeMillis()
    val BBAnalyticRDD = boroughBlockAnalytic(sc, hdfs, path, subwayRDD, crimeRDD, educationRDD, propertyRDD)
    println("BB processing took: " + (System.currentTimeMillis() - now) + " msecs")

    //  Call the method that joins all scores per zipcode
    now = System.currentTimeMillis()
    zipcodeAnalytic(sc, hdfs, path, cleanedPlutoPath, BBAnalyticRDD)
    println("Zipcode processing took: " + (System.currentTimeMillis() - now) + " msecs")
  }

  //  Function that joins all scores per borough-block
  private def boroughBlockAnalytic(sc: SparkContext, hdfs: FileSystem, path: String,
                                   subwayRDD: RDD[(String, (Double, (String, String)))], crimeRDD: RDD[(String, (Double, (String, String)))],
                                   educationRDD: RDD[(String, (Double, (String, String)))], propertyRDD: RDD[(String, Double)]) = {
    val processedBBData = path + PROCESSED_BB_DATA_PATH
    deleteFolderIfAlreadyExists(hdfs, processedBBData)

    //  Joining all scores
    val BBAnalyticRDD = crimeRDD.join(subwayRDD).mapValues(x => (x._1._1, x._2._1, x._1._2))
      .join(educationRDD).mapValues(x => (x._1._1, x._1._2, x._2._1, x._1._3))
      .join(propertyRDD).mapValues(x => (x._1._1, x._1._2, x._1._3, x._2, x._1._4._1, x._1._4._2))

    //  Formatting tuples and removing extra braces to create CSV format
    val processedBbRDD = BBAnalyticRDD.map(x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6))
      .map(x => x.toString().substring(1, x.toString().length - 1))

    //  Final format: "Borough_Block", "Crime Score", "Subway Score", "Education Score", "Average Property Price", "Latitude", "Longitude"
    processedBbRDD.saveAsTextFile(processedBBData)
    BBAnalyticRDD
  }

  //  Function that joins all scores per borough-block
  private def zipcodeAnalytic(sc: SparkContext, hdfs: FileSystem, path: String, cleanedPlutoPath: String,
                              BBAnalyticRDD: RDD[(String, (Double, Double, Double, Double, String, String))]) = {
    val processedZipCodePath = path + PROCESSED_ZIPCODE_DATA_PATH
    deleteFolderIfAlreadyExists(hdfs, processedZipCodePath)

    //  Get borough-blocks for zipcodes
    val zipCodeRDD = PlutoProcess.getBoroughBlockToZipcodeRDD(sc, cleanedPlutoPath)

    //  Create a Map for saving population for each zipcode
    var zipcodePopulationMap = scala.collection.mutable.Map[String, Double]()

    //  Find the population for each zipcode and add to Map
    val population = sc.textFile(path + ZIPCODE_POPULATION_PATH)
      .map(_.split(SPLIT_REGEX))
      .map(x => (x(0), x(1).replace("\"", "").replace(",", "").toDouble))
      .collect().foreach(x => zipcodePopulationMap += (x._1 -> x._2))

    //  Join the Borough-block joined data with the zipcode
    //  Flatmap each grouped record and add 1 for each record
    //  Reduce by zipcode, adding all the scores and adding 1 to get the total number of records per zipcode
    //  Map and divide the values by the total number of scores per zipcode to get the average scores
    //  Add population to this data for each zipcode
    val zipcodeAnalyticRDD = BBAnalyticRDD.join(zipCodeRDD).map(x => (x._2._2, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4)))
      .flatMap(x => x._1.map((_, (x._2, 1.0))))
      .reduceByKey((d1, d2) => ((d1._1._1 + d2._1._1, d1._1._2 + d2._1._2, d1._1._3 + d2._1._3, d1._1._4 + d2._1._4), d1._2 + d2._2))
      .mapValues(x => (x._1._1 / x._2, x._1._2 / x._2, x._1._3 / x._2, x._1._4 / x._2))
      .map(x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4, zipcodePopulationMap.getOrElse(x._1, 0.0))).sortBy(_._6)

    //  Formatting tuples and removing extra braces to create CSV format
    val processedZipcodeRDD = zipcodeAnalyticRDD
      .map(x => x.toString().substring(1, x.toString().length - 1))

    //  Final format: "Zip code", "Crime Score", "Subway Score", "Education Score", "Average Property Price"
    processedZipcodeRDD.saveAsTextFile(processedZipCodePath)
  }
}
