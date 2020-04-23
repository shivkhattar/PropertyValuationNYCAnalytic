package process

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants._

object SubwayProcess {

  def process(sc: SparkContext, cleanedSubwayPath: String, processedPlutoRDD: RDD[Map[String, String]],
              processedPlutoData: List[Map[String, String]]): RDD[(String, (Double, (String, String)))] = {
    val subwayData = sc.textFile(cleanedSubwayPath).map(_.split(SPLIT_REGEX))
      .map(x => Map(OBJECT_ID -> x(0), LATITUDE -> x(2), LONGITUDE -> x(3), LEVEL -> (x(4).split("-").length + 1).toString))
    val totalSubwayScore = subwayData.map(x => ("score", x(LEVEL).toDouble)).reduceByKey(_ + _).map(_._2).collect()(0)
    ProcessUtil.getScoresForData(sc, subwayData, totalSubwayScore, processedPlutoRDD, processedPlutoData, (level: String) => level.toDouble)
  }
}