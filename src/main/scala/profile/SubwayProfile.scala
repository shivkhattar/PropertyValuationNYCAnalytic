package profile

import clean.SubwayClean.getSplitValue
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonUtil

object SubwayProfile extends Profile {

  def profile(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit = {
    val data = sc.textFile(inputPath)
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .map(x => (x(0), x(1), x(2), x(3), x(4)))

    val count = data.map(d => ("Count", 1))
      .reduceByKey(_ + _)
      .map(tup => tup._1 + " : " + tup._2)

    val length = data.map(d => ("Name Length Range", (d._2.length, d._2.length)))
      .reduceByKey((d1, d2) => (if (d1._1 < d2._1) d1._1 else d2._1, if (d1._2 > d2._2) d1._2 else d2._2))
      .map(tup => tup._1 + " : " + tup._2.toString())

    val subwayLines = data.flatMap(d => d._5.split("-"))
    val distinctSubwayLines = subwayLines
      .distinct
      .map(d => ("Distinct Subway Lines", 1))
      .reduceByKey(_ + _)
      .map(tup => tup._1 + " : " + tup._2)

    val countOfSubwayLines = subwayLines.map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + " : " + tup._2)

    val combined = count.union(length).union(distinctSubwayLines)

    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)

    combined.saveAsTextFile(outputPath)

    countOfSubwayLines.saveAsTextFile(outputPath + "/countOfSubwayLines")
  }
}