package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants._

object SubwayProcess {

  def process(sc: SparkContext, hdfs: FileSystem, cleanedSubwayPath: String, processedPlutoData: List[Map[String, String]]): RDD[(String, Double)] = {

    val subwayData = sc.textFile(cleanedSubwayPath).map(_.split(SPLIT_REGEX))
      .map(x => Map(OBJECT_ID -> x(0), LATITUDE -> x(2), LONGITUDE -> x(3), LEVEL -> DEFAULT_LEVEL))

    val totalSubwayScore = subwayData.count()

    ProcessUtil.getScoresForData(sc, subwayData, totalSubwayScore, processedPlutoData, (level: String) => level.toDouble)
  }
}