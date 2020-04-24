package process

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants._

object SubwayProcess {

  def process(sc: SparkContext, cleanedSubwayPath: String, processedPlutoRDD: RDD[Map[String, String]],
              processedPlutoData: List[Map[String, String]]): RDD[(String, (Double, (String, String)))] = {
    val subwayData = sc.textFile(cleanedSubwayPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(OBJECT_ID -> x(0), LATITUDE -> x(2), LONGITUDE -> x(3), LEVEL -> (x(4).split("-").length + 1).toString))

    ProcessUtil.getScoresForData(sc, subwayData, processedPlutoRDD, processedPlutoData, (level: String) => level.toDouble)
  }
}