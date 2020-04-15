import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FileSystem

object App {
  def main(args: Array[String]): Unit = {
    checkArguments(args)
    val conf = new SparkConf().
      setMaster("local[5]").
      setAppName("PropertyValueAnalytic")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val hdfs = FileSystem.get(sc.hadoopConfiguration)

    CleanerAndProfiler.cleanAndProfile(sc, hdfs, args(0))

  }

  private def checkArguments(args: Array[String]) = {
    if (args.size != 1) {
      System.err.println("Incorrect Argument. Please specify the input folder.")
      System.exit(1)
    }
  }
}