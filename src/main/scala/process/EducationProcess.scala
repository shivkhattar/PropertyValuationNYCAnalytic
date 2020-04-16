package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.{ED_ATS_SYSTEM_CODE, LATITUDE, LEVEL, LONGITUDE, SPLIT_REGEX, DEFAULT_LEVEL}

object EducationProcess {
  def process(sc: SparkContext, hdfs: FileSystem, cleanedEducationPath: String, processedPlutoData: List[Map[String, String]]): RDD[(String, Double)] = {
    val educationData = sc.textFile(cleanedEducationPath).map(_.split(SPLIT_REGEX))
      .map(x => Map(ED_ATS_SYSTEM_CODE -> x(0), LATITUDE -> x(2), LONGITUDE -> x(3), LEVEL -> DEFAULT_LEVEL))

    val totalEducationScore = educationData.count()

    ProcessUtil.getScoresForData(sc, educationData, totalEducationScore, processedPlutoData, (level: String) => level.toDouble)
  }
}
