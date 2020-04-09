import clean.{CrimeClean, PlutoClean, SubwayClean}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FileSystem
import profile.{CrimeProfile, PlutoProfile, SubwayProfile}
import util.CommonConstants.{PROFILE_PLUTO_PATH, CLEANED_CRIME_PATH, CLEANED_PLUTO_PATH, CLEANED_SUBWAY_PATH, CRIME_PATH, FILE_SEPARATOR, PLUTO_PATH, PROFILE_CRIME_PATH, PROFILE_SUBWAY_PATH, SUBWAY_PATH}

object App {
  def main(args: Array[String]): Unit = {
    checkArguments(args)
    val start = System.currentTimeMillis();
    val conf = new SparkConf().
      setMaster("local[5]").
      setAppName("PropertyValueAnalytic")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val hdfs = FileSystem.get(sc.hadoopConfiguration)

    val path = getInputPath(args)

    var now = System.currentTimeMillis()
    val crimeInputPath = path + CRIME_PATH
    val crimeOutputPath = path + CLEANED_CRIME_PATH
    CrimeClean.clean(sc, hdfs, crimeInputPath, crimeOutputPath)
    println("Crime Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val subwayInputPath = path + SUBWAY_PATH
    val subwayOutputPath = path + CLEANED_SUBWAY_PATH
    SubwayClean.clean(sc, hdfs, subwayInputPath, subwayOutputPath)
    println("Subway Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val plutoInputPath = path + PLUTO_PATH
    val plutoOutputPath = path + CLEANED_PLUTO_PATH
    PlutoClean.clean(sc, hdfs, plutoInputPath, plutoOutputPath)
    println("Pluto Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val crimeProfileOutputPath = path + PROFILE_CRIME_PATH
    CrimeProfile.profile(sc, hdfs, crimeOutputPath, crimeProfileOutputPath)
    println("Crime Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val subwayProfileOutputPath = path + PROFILE_SUBWAY_PATH
    SubwayProfile.profile(sc, hdfs, subwayOutputPath, subwayProfileOutputPath)
    println("Subway Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val plutoProfileOutputPath = path + PROFILE_PLUTO_PATH
    PlutoProfile.profile(sc, hdfs, plutoOutputPath, plutoProfileOutputPath)
    println("Pluto Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

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