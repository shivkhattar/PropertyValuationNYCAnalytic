import clean.{CrimeClean, EducationClean, PlutoClean, PropertyClean, SubwayClean}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import profile.{CrimeProfile, EducationProfile, PlutoProfile, PropertyProfile, SubwayProfile}
import Analysis.{PlutoLatLong}
import util.CommonConstants.{LATLONG_PLUTO_PATH, PROPERTY_PATH, CLEANED_PROPERTY_PATH, PROFILED_PROPERTY_PATH, EDUCATION_PATH, CLEANED_EDUCATION_PATH, PROFILED_EDUCATION_PATH, CLEANED_CRIME_PATH, CLEANED_PLUTO_PATH, CLEANED_SUBWAY_PATH, CRIME_PATH, FILE_SEPARATOR, PLUTO_PATH, PROFILE_CRIME_PATH, PROFILE_PLUTO_PATH, PROFILE_SUBWAY_PATH, SUBWAY_PATH}

object CleanerAndProfiler {

  def cleanAndProfile(sc: SparkContext, hdfs: FileSystem, inputPath: String): Unit = {
    val start = System.currentTimeMillis()
    var now = start
    val path = getInputPath(inputPath)

    val crimeInputPath = path + CRIME_PATH
    val crimeOutputPath = path + CLEANED_CRIME_PATH
    //CrimeClean.clean(sc, hdfs, crimeInputPath, crimeOutputPath)
    println("Crime Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val crimeProfileOutputPath = path + PROFILE_CRIME_PATH
    //CrimeProfile.profile(sc, hdfs, crimeInputPath, crimeOutputPath, crimeProfileOutputPath)
    println("Crime Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val subwayInputPath = path + SUBWAY_PATH
    val subwayOutputPath = path + CLEANED_SUBWAY_PATH
    //SubwayClean.clean(sc, hdfs, subwayInputPath, subwayOutputPath)
    println("Subway Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val subwayProfileOutputPath = path + PROFILE_SUBWAY_PATH
    //SubwayProfile.profile(sc, hdfs, subwayInputPath, subwayOutputPath, subwayProfileOutputPath)
    println("Subway Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val plutoInputPath = path + PLUTO_PATH
    val plutoOutputPath = path + CLEANED_PLUTO_PATH
    PlutoClean.clean(sc, hdfs, plutoInputPath, plutoOutputPath)
    println("Pluto Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val plutoProfileOutputPath = path + PROFILE_PLUTO_PATH
    //PlutoProfile.profile(sc, hdfs, plutoInputPath, plutoOutputPath, plutoProfileOutputPath)
    println("Pluto Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    //val plutoLatLongOutput = path + LATLONG_PLUTO_PATH
    //PlutoLatLong.getLatLong(sc, hdfs, plutoOutputPath)
    println("Pluto analysis took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val educationInputPath = path + EDUCATION_PATH
    val educationOutputPath = path + CLEANED_EDUCATION_PATH
    EducationClean.clean(sc, hdfs, educationInputPath, educationOutputPath)
    println("Education Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val educationProfileOutputPath = path + PROFILED_EDUCATION_PATH
    //EducationProfile.profile(sc, hdfs, educationInputPath, educationOutputPath, educationProfileOutputPath)
    println("Education Profiling took: " + (System.currentTimeMillis() - now) + " msecs")
    
    now = System.currentTimeMillis()
    val propertyInputPath = path + PROPERTY_PATH
    val propertyOutputPath = path + CLEANED_PROPERTY_PATH
    //PropertyClean.clean(sc, hdfs, propertyInputPath, propertyOutputPath)
    println("Property Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val propertyProfileOutputPath = path + PROFILED_PROPERTY_PATH
    //PropertyProfile.profile(sc, hdfs, propertyInputPath, propertyOutputPath, propertyProfileOutputPath)
    println("Property Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    sc.stop()
    println("Total Application time: " + (System.currentTimeMillis() - start) + " msecs")
    println("Cleaning and Profiling Done!")
  }

  private def getInputPath(inputPath: String) = {
    if (inputPath.endsWith(FILE_SEPARATOR)) inputPath.substring(0, inputPath.length - 1) else inputPath
  }
}
