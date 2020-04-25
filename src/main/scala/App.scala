import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import process.Processor
import util.CommonConstants.FILE_SEPARATOR
import clean.Cleaner
import org.apache.spark.{SparkConf, SparkContext}
import profile.Profiler

object App {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val inputPath = getInputPath(args)

    val conf = new SparkConf().setMaster("yarn").setAppName("PropertyValueAnalytic")

    //    val sess = SparkSession.builder()
    //      .master("yarn")
    //      .appName("PropertyValueAnalytic")
    //      .getOrCreate()

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val hdfs = FileSystem.get(sc.hadoopConfiguration)

    //Cleaner.clean(sc, hdfs, inputPath)
    //Profiler.profile(sc, hdfs, inputPath)
    Processor.process(sc, hdfs, inputPath)
    sc.stop()
    println("Total Application time: " + (System.currentTimeMillis() - start) + " msecs")
  }

  private def checkArguments(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Incorrect Argument. Please specify the input folder.")
      System.exit(1)
    }
  }

  private def getInputPath(args: Array[String]) = {
    checkArguments(args)
    if (args(0).endsWith(FILE_SEPARATOR)) args(0).substring(0, args(0).length - 1) else args(0)
  }
}