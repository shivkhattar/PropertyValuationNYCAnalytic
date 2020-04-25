package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{FINAL_BB_HEADING, FINAL_ZIPCODE_HEADING, PROCESSED_BB_DATA_PATH, PROCESSED_ZIPCODE_DATA_PATH}
import util.CommonUtil.deleteFolderIfAlreadyExists

object AnalyticGenerator {

  def generateAnalytic(sc: SparkContext, hdfs: FileSystem, path: String, cleanedPlutoPath: String, subwayRDD: RDD[(String, (Double, (String, String)))], crimeRDD: RDD[(String, (Double, (String, String)))], educationRDD: RDD[(String, (Double, (String, String)))], propertyRDD: RDD[(String, Double)]) = {
    var now = System.currentTimeMillis()
    val BBAnalyticRDD = boroughBlockAnalytic(sc, hdfs, path, subwayRDD, crimeRDD, educationRDD, propertyRDD)
    println("BB processing took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    zipcodeAnalytic(sc, hdfs, path, cleanedPlutoPath, BBAnalyticRDD)
    println("Zipcode processing took: " + (System.currentTimeMillis() - now) + " msecs")
  }

  // Final format: ("Borough_Block", "Crime Score", "Subway Score", "Education Score", "Average Property Price", "Latitude", "Longitude")
  private def boroughBlockAnalytic(sc: SparkContext, hdfs: FileSystem, path: String, subwayRDD: RDD[(String, (Double, (String, String)))], crimeRDD: RDD[(String, (Double, (String, String)))], educationRDD: RDD[(String, (Double, (String, String)))], propertyRDD: RDD[(String, Double)]) = {
    val processedBBData = path + PROCESSED_BB_DATA_PATH
    deleteFolderIfAlreadyExists(hdfs, processedBBData)

    val BBAnalyticRDD = crimeRDD.join(subwayRDD).mapValues(x => (x._1._1, x._2._1, x._1._2))
      .join(educationRDD).mapValues(x => (x._1._1, x._1._2, x._2._1, x._1._3))
      .join(propertyRDD).mapValues(x => (x._1._1, x._1._2, x._1._3, x._2, x._1._4._1, x._1._4._2)).sortBy(_._2._4)

    val processedBbRDD = BBAnalyticRDD
      .map(x => x.toString().substring(1, x.toString().length - 1))

    processedBbRDD.saveAsTextFile(processedBBData)
    BBAnalyticRDD
  }

  // Final format: ("Zip code", "Crime Score", "Subway Score", "Education Score", "Average Property Price")
  private def zipcodeAnalytic(sc: SparkContext, hdfs: FileSystem, path: String, cleanedPlutoPath: String, BBAnalyticRDD: RDD[(String, (Double, Double, Double, Double, String, String))]) = {
    val processedZipCodePath = path + PROCESSED_ZIPCODE_DATA_PATH
    deleteFolderIfAlreadyExists(hdfs, processedZipCodePath)

    val zipCodeRDD = PlutoProcess.getBoroughBlockToZipcodeRDD(sc, cleanedPlutoPath)

    val zipcodeAnalyticRDD = BBAnalyticRDD.join(zipCodeRDD).map(x => (x._2._2, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4)))
      .flatMap(x => x._1.map((_, (x._2, 1.0))))
      .reduceByKey((d1, d2) => ((d1._1._1 + d2._1._1, d1._1._2 + d2._1._2, d1._1._3 + d2._1._3, d1._1._4 + d2._1._4), d1._2 + d2._2))
      .mapValues(x => (x._1._1 / x._2, x._1._2 / x._2, x._1._3 / x._2, x._1._4 / x._2))
      .map(x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4))

    val processedZipcodeRDD = zipcodeAnalyticRDD
      .map(x => x.toString().substring(1, x.toString().length - 1))

    processedZipcodeRDD.saveAsTextFile(processedZipCodePath)
  }
}
