package clean


import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import util.CommonUtil

object CrimeClean extends Clean {

  def clean(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit = {
    val data = sc.textFile(inputPath).map(d => if (d.endsWith(",")) d.concat(CommonUtil.unknown) else d)
    val rowsRemoved = data.map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .filter(_.length == 35)
      .map(x => (x(0), x(6), x(8), x(12), x(13), x(15), x(16), x(27), x(28), x(21), x(22), x(23), x(24), x(25), x(32), x(33), x(34)))

    val cleanedCrime = rowsRemoved.filter(tup => !tup._1.isEmpty && !tup._2.isEmpty && !tup._8.isEmpty && !tup._9.isEmpty && !tup._10.isEmpty && !tup._11.isEmpty)
      .map(tup => (tup._1, tup._2, CommonUtil.updateValueIfBlank(tup._3), CommonUtil.updateValueIfBlank(tup._4), CommonUtil.updateValueIfBlank(tup._5), CommonUtil.updateValueIfBlank(tup._6), CommonUtil.updateValueIfBlank(tup._7), tup._8, tup._9, tup._10, tup._11, CommonUtil.updateValueIfBlank(tup._12), CommonUtil.updateValueIfBlank(tup._13), CommonUtil.updateValueIfBlank(tup._14), CommonUtil.updateValueIfBlank(tup._15), CommonUtil.updateValueIfBlank(tup._16), CommonUtil.updateValueIfBlank(tup._17)))
      .map(tup => tup.toString.substring(1, tup.toString.length - 1))

    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    cleanedCrime.saveAsTextFile(outputPath)
  }
}