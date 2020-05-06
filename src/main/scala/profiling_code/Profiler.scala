/**
 * @author Shiv Khattar(sk8325)
 * @author Rutvik Shah(rss638)
 */

package profiling_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util_code.CommonConstants._

//  Service that handles all the profiling jobs
object Profiler {

  //  Function that handles all the profiling jobs
  def profile(sc: SparkContext, hdfs: FileSystem, path: String): Unit = {
    var now = System.currentTimeMillis()

    //  Call the function which takes care of profiling for the Crime dataset
    val crimeInputPath = path + CRIME_PATH
    val crimeOutputPath = path + CLEANED_CRIME_PATH
    val crimeProfileOutputPath = path + PROFILE_CRIME_PATH
    CrimeProfile.profile(sc, hdfs, crimeInputPath, crimeOutputPath, crimeProfileOutputPath)
    println("Crime Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    //  Call the function which takes care of profiling for the Subway dataset
    now = System.currentTimeMillis()
    val subwayInputPath = path + SUBWAY_PATH
    val subwayOutputPath = path + CLEANED_SUBWAY_PATH
    val subwayProfileOutputPath = path + PROFILE_SUBWAY_PATH
    SubwayProfile.profile(sc, hdfs, subwayInputPath, subwayOutputPath, subwayProfileOutputPath)
    println("Subway Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    //  Call the function which takes care of profiling for the Education dataset
    now = System.currentTimeMillis()
    val educationInputPath = path + EDUCATION_PATH
    val educationOutputPath = path + CLEANED_EDUCATION_PATH
    val educationProfileOutputPath = path + PROFILED_EDUCATION_PATH
    EducationProfile.profile(sc, hdfs, educationInputPath, educationOutputPath, educationProfileOutputPath)
    println("Education Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    //  Call the function which takes care of profiling for the Property dataset
    now = System.currentTimeMillis()
    val propertyInputPath = path + PROPERTY_PATH
    val propertyOutputPath = path + CLEANED_PROPERTY_PATH
    val propertyProfileOutputPath = path + PROFILED_PROPERTY_PATH
    PropertyProfile.profile(sc, hdfs, propertyInputPath, propertyOutputPath, propertyProfileOutputPath)
    println("Property Profiling took: " + (System.currentTimeMillis() - now) + " msecs")
    println("Profiling Done!")

    //  Call the function which takes care of profiling for the Pluto dataset
    now = System.currentTimeMillis()
    val plutoInputPath = path + PLUTO_PATH
    val plutoOutputPath = path + CLEANED_PLUTO_PATH
    val plutoProfileOutputPath = path + PROFILE_PLUTO_PATH
    PlutoProfile.profile(sc, hdfs, plutoInputPath, plutoOutputPath, plutoProfileOutputPath)
    println("Pluto Profiling took: " + (System.currentTimeMillis() - now) + " msecs")
  }
}