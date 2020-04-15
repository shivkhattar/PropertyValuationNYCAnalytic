import clean.{EducationCleanFinal, PropertyCleanFinal}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FileSystem
import profile.{EducationProfile, PropertyProfile}
import util.EdPropConstants.{CLEANED_EDUCATION_PATH, CLEANED_PROPERTY_PATH, EDUCATION_PATH, FILE_SEPARATOR, PROFILED_EDUCATION_PATH, PROFILED_PROPERTY_PATH, PROPERTY_PATH}

object AppR {
  def main(args: Array[String]): Unit = {
    checkArguments(args)
    val start = System.currentTimeMillis()
    val conf = new SparkConf().
      setMaster("yarn").
      setAppName("PropertyValueAnalytic")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val hdfs = FileSystem.get(sc.hadoopConfiguration)

    val path = getInputPath(args)
    //val path = "C:/Users/rss16/Documents/PropertyValuationNYCAnalytic/"

    var now = System.currentTimeMillis()
    val educationInputPath = path + EDUCATION_PATH
    val educationOutputPath = path + CLEANED_EDUCATION_PATH
    EducationCleanFinal.clean(sc, hdfs, educationInputPath, educationOutputPath)
    println("Education Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val propertyInputPath = path + PROPERTY_PATH
    val propertyOutputPath = path + CLEANED_PROPERTY_PATH
    PropertyCleanFinal.clean(sc, hdfs, propertyInputPath, propertyOutputPath)
    println("Property Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val educationProfileOutputPath = path + PROFILED_EDUCATION_PATH
    EducationProfile.profile(sc, hdfs, educationInputPath, educationOutputPath, educationProfileOutputPath)
    println("Education Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val propertyProfileOutputPath = path + PROFILED_PROPERTY_PATH
    PropertyProfile.profile(sc, hdfs, propertyInputPath, propertyOutputPath, propertyProfileOutputPath)
    println("Property Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    sc.stop()
    println("Total Application time: " + (System.currentTimeMillis() - start) + " msecs")
    println("Cleaning and Profiling Done!")
  }

  private def checkArguments(args: Array[String]) = {
    if (args.size != 1) {
      System.err.println("Incorrect Argument. Please specify the input folder.")
      System.exit(1)
    }
  }

  private def getInputPath(args: Array[String]) = {
    if (args(0).endsWith(FILE_SEPARATOR)) args(0).substring(0, args(0).length - 1) else args(0)
  }

}