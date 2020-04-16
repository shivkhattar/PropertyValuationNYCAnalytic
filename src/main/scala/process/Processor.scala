package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonConstants.{CLEANED_PLUTO_PATH, CLEANED_SUBWAY_PATH, CLEANED_CRIME_PATH, CLEANED_EDUCATION_PATH}

object Processor {

  def preprocess(sc: SparkContext, hdfs: FileSystem, path: String): Unit = {
    var now = System.currentTimeMillis()

    val cleanedPlutoPath = path + CLEANED_PLUTO_PATH
    val plutoData = PlutoProcess.normalizeLatLongFromRDD(sc, hdfs, cleanedPlutoPath)
    println("Pluto preprocessing took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val cleanedSubwayPath = path + CLEANED_SUBWAY_PATH
    //val subwayRDD = SubwayProcess.process(sc, hdfs, cleanedSubwayPath, plutoData)
    println("Subway preprocessing took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val cleanedCrimePath = path + CLEANED_CRIME_PATH + "2"
    val crimeRDD = CrimeProcess.process(sc, hdfs, cleanedCrimePath, plutoData)
    println("Crime preprocessing took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val cleanedEducationPath = path + CLEANED_EDUCATION_PATH
    //val educationRDD = EducationProcess.process(sc, hdfs, cleanedEducationPath, plutoData)
    println("Education preprocessing took: " + (System.currentTimeMillis() - now) + " msecs")
  }

}
