/**
 * @author Shiv Khattar(sk8325)
 * @author Rutvik Shah(rss638)
 */

package analytic_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util_code.CommonConstants.{CLEANED_CRIME_PATH, CLEANED_EDUCATION_PATH, CLEANED_PLUTO_PATH, CLEANED_PROPERTY_PATH, CLEANED_SUBWAY_PATH}

//  Service that controls all the processing logic
object Processor {

  // Function that controls all the process jobs
  def process(sc: SparkContext, hdfs: FileSystem, path: String): Unit = {

    //  Call the function that controls processing of Pluto dataset
    val now = System.currentTimeMillis()
    val cleanedPlutoPath = path + CLEANED_PLUTO_PATH
    val plutoRDDAndData = PlutoProcess.process(sc, cleanedPlutoPath)
    println("Pluto preprocessing took: " + (System.currentTimeMillis() - now) + " msecs")

    val plutoRDD = plutoRDDAndData._1
    val plutoData = plutoRDDAndData._2

    //  Call the function that controls processing of subway dataset
    val cleanedSubwayPath = path + CLEANED_SUBWAY_PATH
    val subwayRDD = SubwayProcess.process(sc, cleanedSubwayPath, plutoRDD, plutoData)

    //  Call the function that controls processing of crime dataset
    val cleanedCrimePath = path + CLEANED_CRIME_PATH
    val crimeRDD = CrimeProcess.process(sc, cleanedCrimePath, plutoRDD, plutoData)

    //  Call the function that controls processing of education dataset
    val cleanedEducationPath = path + CLEANED_EDUCATION_PATH
    val educationRDD = EducationProcess.process(sc, cleanedEducationPath, plutoRDD, plutoData)

    //  Call the function that controls processing of property dataset
    val cleanedPropertyPath = path + CLEANED_PROPERTY_PATH
    val propertyRDD = PropertyProcess.process(sc, cleanedPropertyPath)

    //  Call the function which generates the analytic, by passing all the processed RDDs
    AnalyticGenerator.generateAnalytic(sc, hdfs, path, cleanedPlutoPath, subwayRDD, crimeRDD, educationRDD, propertyRDD)
  }
}