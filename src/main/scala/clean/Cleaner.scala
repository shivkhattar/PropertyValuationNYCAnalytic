package clean

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonConstants._
object Cleaner {

  def clean(sc: SparkContext, hdfs: FileSystem, path: String): Unit = {
    var now = System.currentTimeMillis()

    val crimeInputPath = path + CRIME_PATH
    val crimeOutputPath = path + CLEANED_CRIME_PATH
    CrimeClean.clean(sc, hdfs, crimeInputPath, crimeOutputPath)
    println("Crime Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val subwayInputPath = path + SUBWAY_PATH
    val subwayOutputPath = path + CLEANED_SUBWAY_PATH
    //SubwayClean.clean(sc, hdfs, subwayInputPath, subwayOutputPath)
    println("Subway Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val plutoInputPath = path + PLUTO_PATH
    val plutoOutputPath = path + CLEANED_PLUTO_PATH
    //PlutoClean.clean(sc, hdfs, plutoInputPath, plutoOutputPath)
    println("Pluto Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val educationInputPath = path + EDUCATION_PATH
    val educationOutputPath = path + CLEANED_EDUCATION_PATH
    //EducationClean.clean(sc, hdfs, educationInputPath, educationOutputPath)
    println("Education Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val propertyInputPath = path + PROPERTY_PATH
    val propertyOutputPath = path + CLEANED_PROPERTY_PATH
    //PropertyClean.clean(sc, hdfs, propertyInputPath, propertyOutputPath)
    println("Property Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    println("Cleaning Done!")
  }
}
