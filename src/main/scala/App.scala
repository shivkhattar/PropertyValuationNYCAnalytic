import clean.{CrimeClean, SubwayClean}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FileSystem
import profile.{CrimeProfile, SubwayProfile}

object App {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis();
    val conf = new SparkConf().
      setMaster("local[5]").
      setAppName("CleaningAndProfiling")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val hdfs = FileSystem.get(sc.hadoopConfiguration)

    var now = System.currentTimeMillis()
    val crimeInputPath = "data/crime.csv"
    val crimeOutputPath = "data/output/cleaned/crime"
    CrimeClean.clean(sc, hdfs, crimeInputPath, crimeOutputPath)
    println("Crime Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val subwayInputPath = "data/subway.csv"
    val subwayOutputPath = "data/output/cleaned/subway"
    SubwayClean.clean(sc, hdfs, subwayInputPath, subwayOutputPath)
    println("Subway Cleaning took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val crimeProfileOutputPath = "data/output/profile/crime"
    CrimeProfile.profile(sc, hdfs, crimeOutputPath, crimeProfileOutputPath)
    println("Crime Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    now = System.currentTimeMillis()
    val subwayProfileOutputPath = "data/output/profile/subway"
    SubwayProfile.profile(sc, hdfs, subwayOutputPath, subwayProfileOutputPath)
    println("Subway Profiling took: " + (System.currentTimeMillis() - now) + " msecs")

    sc.stop()
    println("Cleaning and Profiling Done!")
    println("Total Application time: " + (System.currentTimeMillis() - start) + " msecs")
  }
}
