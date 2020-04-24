package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonConstants.{CLEANED_CRIME_PATH, CLEANED_EDUCATION_PATH, CLEANED_PLUTO_PATH, CLEANED_PROPERTY_PATH, CLEANED_SUBWAY_PATH}

object Processor {

  def process(sc: SparkContext, hdfs: FileSystem, path: String): Unit = {
    val now = System.currentTimeMillis()

    val cleanedPlutoPath = path + CLEANED_PLUTO_PATH
    val plutoRDDAndData = PlutoProcess.normalizeLatLongFromRDD(sc, cleanedPlutoPath)
    println("Pluto preprocessing took: " + (System.currentTimeMillis() - now) + " msecs")

    val plutoRDD = plutoRDDAndData._1;
    val plutoData = plutoRDDAndData._2;

    val cleanedSubwayPath = path + CLEANED_SUBWAY_PATH
    val subwayRDD = SubwayProcess.process(sc, cleanedSubwayPath, plutoRDD, plutoData)

    val cleanedCrimePath = path + CLEANED_CRIME_PATH + "2"
    val crimeRDD = CrimeProcess.process(sc, cleanedCrimePath, plutoRDD, plutoData)

    val cleanedEducationPath = path + CLEANED_EDUCATION_PATH
    val educationRDD = EducationProcess.process(sc, cleanedEducationPath, plutoRDD, plutoData)

    val cleanedPropertyPath = path + CLEANED_PROPERTY_PATH
    val propertyRDD = PropertyProcess.process(sc, cleanedPropertyPath)

    AnalyticGenerator.generateAnalytic(sc, hdfs, path, cleanedPlutoPath, subwayRDD, crimeRDD, educationRDD, propertyRDD)
  }
}