package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonConstants.{CLEANED_CRIME_PATH, CLEANED_EDUCATION_PATH, CLEANED_PLUTO_PATH, CLEANED_PROPERTY_PATH, CLEANED_SUBWAY_PATH, FINAL_HEADING, PROCESSED_DATA_PATH}
import util.CommonUtil.deleteFolderIfAlreadyExists

object Processor {

  def preprocess(sc: SparkContext, hdfs: FileSystem, path: String): Unit = {
    var now = System.currentTimeMillis()

    val cleanedPlutoPath = path + CLEANED_PLUTO_PATH
    val plutoRDDAndData = PlutoProcess.normalizeLatLongFromRDD(sc, cleanedPlutoPath)
    println("Pluto preprocessing took: " + (System.currentTimeMillis() - now) + " msecs")

    val plutoRDD = plutoRDDAndData._1;
    val plutoData = plutoRDDAndData._2;
    now = System.currentTimeMillis()
    val cleanedSubwayPath = path + CLEANED_SUBWAY_PATH
    val subwayRDD = SubwayProcess.process(sc, cleanedSubwayPath, plutoRDD, plutoData)

    val cleanedCrimePath = path + CLEANED_CRIME_PATH + "2"
    val crimeRDD = CrimeProcess.process(sc, cleanedCrimePath, plutoRDD, plutoData)

    val cleanedEducationPath = path + CLEANED_EDUCATION_PATH
    val educationRDD = EducationProcess.process(sc, cleanedEducationPath, plutoRDD, plutoData)

    val cleanedPropertyPath = path + CLEANED_PROPERTY_PATH
    val propertyRDD = PropertyProcess.process(sc, cleanedPropertyPath)

    now = System.currentTimeMillis()
    val finalProcessedPath = path + PROCESSED_DATA_PATH
    deleteFolderIfAlreadyExists(hdfs, finalProcessedPath)

    // Joining all RDDs; Final format: ("Borough_Block", "Latitude", "Longitude", "Crime Score", "Subway Score", "Education Score", "Average Property Price")
    val joined = crimeRDD.join(subwayRDD).mapValues(x => (x._1._1, x._2._1, x._1._2))
      .join(educationRDD).map(x => (x._1, (x._2._1._3._1, x._2._1._3._2, x._2._1._1.toString, x._2._1._2.toString, x._2._2._1.toString)))
      .join(propertyRDD).map(x => (x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._2.toString))

    val finalRDD = sc.parallelize(FINAL_HEADING).union(joined)
    finalRDD.saveAsTextFile(finalProcessedPath)
    println("Final processing took: " + (System.currentTimeMillis() - now) + " msecs")
  }
}