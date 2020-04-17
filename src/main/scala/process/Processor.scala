package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import util.CommonConstants.{CLEANED_CRIME_PATH, CLEANED_EDUCATION_PATH, CLEANED_PLUTO_PATH, CLEANED_SUBWAY_PATH, CLEANED_PROPERTY_PATH, FINAL_HEADING}

object Processor {

  def preprocess(sc: SparkContext, hdfs: FileSystem, sess: SparkSession, path: String): Unit = {
    var now = System.currentTimeMillis()

    val cleanedPlutoPath = path + CLEANED_PLUTO_PATH
    val plutoData = PlutoProcess.normalizeLatLongFromRDD(sc, hdfs, cleanedPlutoPath)
    println("Pluto preprocessing took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val cleanedSubwayPath = path + CLEANED_SUBWAY_PATH
    //val subwayRDD = SubwayProcess.process(sc, hdfs, cleanedSubwayPath, plutoData)

    val cleanedCrimePath = path + CLEANED_CRIME_PATH
    ///val crimeRDD = CrimeProcess.process(sc, hdfs, cleanedCrimePath, plutoData)

    val cleanedEducationPath = path + CLEANED_EDUCATION_PATH
    //val educationRDD = EducationProcess.process(sc, hdfs, cleanedEducationPath, plutoData)

    val cleanedPropertyPath = path + CLEANED_PROPERTY_PATH
    val propertyRDD = PropertyProcess.process(sc, hdfs, sess, cleanedPropertyPath)

    now = System.currentTimeMillis()
    //val joined = crimeRDD.join(subwayRDD).mapValues(x => (x._1._1, x._2._1, x._1._2)).join(educationRDD).map(x => (x._1, x._2._1._3._1, x._2._1._3._2, x._2._1._1.toString, x._2._1._2.toString, x._2._2._1.toString))
    //val finalRDD = sc.parallelize(FINAL_HEADING).union(joined)
    //finalRDD.saveAsTextFile(path+"/final")
    println("Final processing took: " + (System.currentTimeMillis() - now) + " msecs")
  }
}