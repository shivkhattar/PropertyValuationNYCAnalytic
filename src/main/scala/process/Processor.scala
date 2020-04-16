package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonConstants.{CLEANED_PLUTO_PATH, CLEANED_SUBWAY_PATH}

object Processor {

  def preprocess(sc: SparkContext, hdfs: FileSystem, path: String): Unit = {
    var now = System.currentTimeMillis()

    val cleanedPlutoPath = path + CLEANED_PLUTO_PATH
    val plutoData = PlutoProcess.normalizeLatLongFromRDD(sc, hdfs, cleanedPlutoPath)
    println("Pluto preprocessing took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val cleanedSubwayPath = path + CLEANED_SUBWAY_PATH
    val subwayRDD = SubwayProcess.processSubway(sc, hdfs, cleanedSubwayPath, plutoData)
    println("Subway preprocessing took: " + (System.currentTimeMillis() - now) + " msecs")

  }

}
