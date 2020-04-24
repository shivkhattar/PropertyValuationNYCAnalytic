package process

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants._

object CrimeProcess {

  def process(sc: SparkContext, cleanedCrimePath: String, processedPlutoRDD: RDD[Map[String, String]],
              processedPlutoData: List[Map[String, String]]): RDD[(String, (Double, (String, String)))] = {
    val crimeData = sc.textFile(cleanedCrimePath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(CMPLNT_NUM -> x(0), LEVEL -> x(3), LATITUDE -> x(5), LONGITUDE -> x(6)))
    ProcessUtil.getScoresForData(sc, crimeData, processedPlutoRDD, processedPlutoData, getScoreForLevel)
  }

  def getScoreForLevel(level: String): Double = {
    if (level.equals("MISDEMEANOR")) 1 else if (level.equals("VIOLATION")) 2 else if (level.equals("FELONY")) 3 else 0
  }
}
