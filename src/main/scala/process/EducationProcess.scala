package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{BOROUGH_BLOCK, LATITUDE, LONGITUDE, OBJECT_ID, SPLIT_REGEX, ZERO_SCORE}
import util.CommonUtil

object EducationProcess {
  def processEducation(sc: SparkContext, hdfs: FileSystem, cleanedEducationPath: String, processedPlutoData: List[Map[String, String]]): RDD[(String, Double)] = {

    val educationData = sc.textFile(cleanedEducationPath).map(_.split(SPLIT_REGEX))
      .map(x => Map(OBJECT_ID -> x(0), LATITUDE -> x(2), LONGITUDE -> x(3)))

    val totalEducationScore = educationData.count()

    val educationScoreRDD = educationData.map(row => processedPlutoData
      .map(y => (y(BOROUGH_BLOCK), CommonUtil.calculateDistance((row(LATITUDE), row(LONGITUDE)), (y(LATITUDE), y(LONGITUDE))), 1))
      .minBy(_._2))
      .map(x => (x._1, if (CommonUtil.inRange(x._2)) x._3 else ZERO_SCORE)).reduceByKey(_ + _).map(x => (x._1, x._2.toDouble / totalEducationScore))

    println(processedPlutoData.length)
    val pluto = sc.parallelize(processedPlutoData).map(y => (y(BOROUGH_BLOCK), None))
    val ansRDD = educationScoreRDD.fullOuterJoin(pluto).mapValues(option => if (option._1.isEmpty) ZERO_SCORE else option._1.get).filter(row => row._2 > 0.0 ).count

    educationScoreRDD.fullOuterJoin(pluto).mapValues(option => if (option._1.isEmpty) ZERO_SCORE else option._1.get)
  }
}
