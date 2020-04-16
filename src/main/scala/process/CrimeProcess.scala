package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants._

object CrimeProcess {

  def process(sc: SparkContext, hdfs: FileSystem, cleanedCrimePath: String, processedPlutoData: List[Map[String, String]]): RDD[(String, Double)] = {
    val crimeData = sc.textFile(cleanedCrimePath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(CMPLNT_NUM -> x(0), LEVEL -> x(3), LATITUDE -> x(5), LONGITUDE -> x(6)))
    val totalCrimeScore = crimeData.map(x => ("score", getScoreForLevel(x(LEVEL).trim))).reduceByKey(_ + _).map(_._2).collect()(0)
    ProcessUtil.getScoresForData(sc, crimeData, totalCrimeScore, processedPlutoData, getScoreForLevel)
  }

  def getScoreForLevel(level: String): Double = {
    if (level.equals("MISDEMEANOR")) 1 else if (level.equals("VIOLATION")) 2 else if (level.equals("FELONY")) 3 else 0
  }
}
