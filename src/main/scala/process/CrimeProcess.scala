package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonConstants._
import util.CommonUtil

object CrimeProcess {

  def processCrime(sc: SparkContext, hdfs: FileSystem, cleanedCrimePath: String, processedPlutoPath: String, outputPath: String): Unit = {

    val crime = List("1234, MISDEMEANOR, 53.32055555555556, -1.7297222222222221", "345,VIOLATION,40.5539605011421,-74.1369580011594")
    val pluto = List("qwer, 53.31861111111111, -1.6997222222222223", "fgmgef,40.7074514457511,-73.8967321062315")
    val crimeData = sc.parallelize(crime)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(CMPLNT_NUM -> x(0), LEVEL -> x(1), LATITUDE -> x(2), LONGITUDE -> x(3)))

    val totalCrimeScore = crimeData.map(x => ("score", getScoreForLevel(x(LEVEL).trim))).reduceByKey(_ + _).map(_._2).collect()(0)

    val plutoData = sc.parallelize(pluto).map(_.split(SPLIT_REGEX))
      .map(x => Map(BOROUGH_BLOCK -> x(0), LATITUDE -> x(1), LONGITUDE -> x(2))).collect

    val crimeScoreRDD = crimeData.map(row => plutoData
      .map(y => (y(BOROUGH_BLOCK), CommonUtil.calculateDistance((row(LATITUDE), row(LONGITUDE)), (y(LATITUDE), y(LONGITUDE))), getScoreForLevel(row(LEVEL).trim)))
      .filter(y => CommonUtil.inRange(y._2))
      .toList.minBy(_._2))
      .map(x => (x._1, x._3)).reduceByKey(_ + _).map(x => (x._1, x._2.toDouble / totalCrimeScore))

    crimeScoreRDD.take(10).foreach(println)
  }

  def getScoreForLevel(level: String): Double = {
    if (level.equals("MISDEMEANOR")) 1 else if (level.equals("VIOLATION")) 2 else if (level.equals("FELONY")) 3 else 0
  }
}
