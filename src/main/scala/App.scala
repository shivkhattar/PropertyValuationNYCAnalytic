import clean.Cleaner
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FileSystem
import profile.Profiler
import process.Processor
import util.CommonConstants.FILE_SEPARATOR

object App {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val inputPath = getInputPath(args)
    val conf = new SparkConf().
      setMaster("local[5]").
      setAppName("PropertyValueAnalytic")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val hdfs = FileSystem.get(sc.hadoopConfiguration)

//    Cleaner.clean(sc, hdfs, inputPath)
//    Profiler.profile(sc, hdfs, inputPath)
    Processor.preprocess(sc, hdfs, inputPath)
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