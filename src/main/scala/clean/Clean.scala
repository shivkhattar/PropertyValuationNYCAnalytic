package clean

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext

abstract class Clean {
  def clean(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit
}