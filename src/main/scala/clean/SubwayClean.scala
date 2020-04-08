package clean

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import util.CommonUtil

object SubwayClean extends Clean {

  def clean(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String) = {
    val data = sc.textFile(inputPath)
    val rowsRemoved = data.map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .filter(_.length == 6)
      .map(x => (x(1), x(2), x(3), x(4)))

    val cleanedSubway = rowsRemoved.filter(tup => !tup._1.isBlank && !tup._2.isBlank && !tup._3.isBlank)
      .map(tup => (tup._1, tup._2, getSplitValue(tup._3, true), getSplitValue(tup._3, false), CommonUtil.updateValueIfBlank(tup._4)))
      .map(tup => tup.toString.substring(1, tup.toString.length - 1))
    println(cleanedSubway.count())
    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    cleanedSubway.saveAsTextFile(outputPath)
  }

  def getSplitValue( geom : String, lat : Boolean): String = {
    val split = geom.replace("POINT (", "").replace( ")", "")
    if(lat) split.substring(0, split.indexOf(" ")+1).replace(" ", "")
    else split.substring(split.indexOf(" ")+1)
  }
}