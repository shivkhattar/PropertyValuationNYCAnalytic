/**
 * @author Shiv Khattar(sk8325)
 * @author Rutvik Shah(rss638)
 */

import org.apache.hadoop.fs.FileSystem
import analytic_code.Processor
import util_code.CommonConstants.FILE_SEPARATOR
import etl_code.Cleaner
import org.apache.spark.{SparkConf, SparkContext}
import profiling_code.Profiler

object App {

  //  Main entry point of the Application
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val inputPath = getInputPath(args)

    val conf = new SparkConf().setMaster("yarn").setAppName("PropertyValueAnalytic")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val hdfs = FileSystem.get(sc.hadoopConfiguration)

    //  Call the cleaner service
    Cleaner.clean(sc, hdfs, inputPath)

    //  Call the profiler service
    Profiler.profile(sc, hdfs, inputPath)

    //  Call the processor service
    Processor.process(sc, hdfs, inputPath)
    sc.stop()
    println("Total Application time: " + (System.currentTimeMillis() - start) + " msecs")
  }

  //  Helper function to check if the arguments are correct or not
  private def checkArguments(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Incorrect Argument. Please specify the input folder.")
      System.exit(1)
    }
  }

  //  Helper function to return the input file in the correct format
  private def getInputPath(args: Array[String]) = {
    checkArguments(args)
    if (args(0).endsWith(FILE_SEPARATOR)) args(0).substring(0, args(0).length - 1) else args(0)
  }
}