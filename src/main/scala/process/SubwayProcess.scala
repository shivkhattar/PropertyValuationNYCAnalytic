package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonConstants._
import util.CommonUtil

object SubwayProcess {

  def processSubway(sc: SparkContext, hdfs: FileSystem, cleanedSubwayPath: String, processedPlutoData: List[Map[String, String]], outputPath: String): Unit = {
    val subwayData = sc.textFile(cleanedSubwayPath).map(_.split(SPLIT_REGEX))
      .map(x => Map(OBJECT_ID -> x(0), LATITUDE -> x(2), LONGITUDE -> x(3)))

    val totalSubwayScore = subwayData.count()

    println(totalSubwayScore)
    println(processedPlutoData.length)
    val subwayScoreRDD = subwayData.map(row => processedPlutoData
      .map(y => (y(BOROUGH_BLOCK), CommonUtil.calculateDistance((row(LATITUDE), row(LONGITUDE)), (y(LATITUDE), y(LONGITUDE))), 1))
      .minBy(_._2))
      .map(x => (x._1, x._3)).reduceByKey(_ + _).map(x => (x._1, x._2.toDouble / totalSubwayScore))

    println(subwayScoreRDD.count())

  }
}
