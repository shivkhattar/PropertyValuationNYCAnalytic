/**
 * @author Shiv Khattar(sk8325)
 */

package etl_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext

//  Abstract class that declares the clean function that needs to be invoked by all individual cleaning objects
abstract class Clean {
  def clean(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit
}