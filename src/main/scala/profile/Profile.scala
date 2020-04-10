package profile

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext

abstract class Profile {

  def profile(sc: SparkContext, hdfs: FileSystem, originalInputPath: String, cleanedInputPath: String, outputPath: String): Unit
}
