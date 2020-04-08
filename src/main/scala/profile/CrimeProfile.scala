package profile

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext

object CrimeProfile extends Profile {

  def profile(sc: SparkContext, hdfs: FileSystem, inputPath : String, outputPath: String): Unit = {
    val outputPath = "data/output/profiledSubway"


  }
}
