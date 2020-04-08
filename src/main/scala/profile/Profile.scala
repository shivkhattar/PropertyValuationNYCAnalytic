package profile

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext

abstract class Profile {

  def profile(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit
}
