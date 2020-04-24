package process

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{ED_ATS_SYSTEM_CODE, LATITUDE, LEVEL, LONGITUDE, SPLIT_REGEX, DEFAULT_LEVEL}

object EducationProcess {
  def process(sc: SparkContext, cleanedEducationPath: String, processedPlutoRDD: RDD[Map[String, String]],
              processedPlutoData: List[Map[String, String]]): RDD[(String, (Double, (String, String)))] = {
    val educationData = sc.textFile(cleanedEducationPath).map(_.split(SPLIT_REGEX))
      .map(x => Map(ED_ATS_SYSTEM_CODE -> x(0), LATITUDE -> x(2), LONGITUDE -> x(3), LEVEL -> DEFAULT_LEVEL))

    ProcessUtil.getScoresForData(sc, educationData, processedPlutoRDD, processedPlutoData, (level: String) => level.toDouble)
  }
}
