/**
 * @author Rutvik Shah(rss638)
 */

package profiling_code

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext

//  Abstract class that declares the profile function that needs to be invoked by all individual cleaning objects
abstract class Profile {

  def profile(sc: SparkContext, hdfs: FileSystem, originalInputPath: String, cleanedInputPath: String, outputPath: String): Unit
}
