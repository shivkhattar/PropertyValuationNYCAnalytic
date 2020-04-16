package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants._
import util.CommonUtil

object SubwayProcess {

  def processSubway(sc: SparkContext, hdfs: FileSystem, cleanedSubwayPath: String, processedPlutoData: List[Map[String, String]], outputPath: String): RDD[(String, Double)] = {

    val subwayData = sc.textFile(cleanedSubwayPath).map(_.split(SPLIT_REGEX))
      .map(x => Map(OBJECT_ID -> x(0), LATITUDE -> x(2), LONGITUDE -> x(3)))

    val totalSubwayScore = subwayData.count()

    val subwayScoreRDD = subwayData.map(row => processedPlutoData
      .map(y => (y(BOROUGH_BLOCK), CommonUtil.calculateDistance((row(LATITUDE), row(LONGITUDE)), (y(LATITUDE), y(LONGITUDE))), 1))
      .minBy(_._2))
      .map(x => (x._1, if (CommonUtil.inRange(x._2)) x._3 else ZERO_SCORE)).reduceByKey(_ + _).map(x => (x._1, x._2.toDouble / totalSubwayScore))

    val pluto = sc.parallelize(processedPlutoData).map(y => (y(BOROUGH_BLOCK), None))

    subwayScoreRDD.fullOuterJoin(pluto).mapValues(option => if (option._1.isEmpty) ZERO_SCORE else option._1.get)
  }
}