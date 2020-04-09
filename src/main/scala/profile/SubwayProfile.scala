package profile

import clean.SubwayClean.getSplitValue
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonUtil

object SubwayProfile extends Profile {

  def profile(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit = {
    val data = sc.textFile(inputPath)
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .map(x => (x(0), x(1), x(2), x(3), x(4)))

    val count = getTotalCount(data)
    val nameLengthRange = getNameLengthRange(data)
    val subwayLines = data.flatMap(d => d._5.split("-"))
    val distinctSubwayLines = getDistinctSubwayLines(subwayLines)
    val countOfSubwayLines = getCountOfSubwayLines(subwayLines)
    
    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)

    val combined = count.union(nameLengthRange).union(distinctSubwayLines)
    combined.saveAsTextFile(outputPath)


    countOfSubwayLines.saveAsTextFile(outputPath + "/countOfSubwayLines")
  }

  private def getCountOfSubwayLines(subwayLines: RDD[String]) = {
    val countOfSubwayLines = subwayLines.map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + " : " + tup._2)
    countOfSubwayLines
  }

  private def getDistinctSubwayLines(subwayLines: RDD[String]) = {
    val distinctSubwayLines = subwayLines
      .distinct
      .map(d => ("Distinct Subway Lines", 1))
      .reduceByKey(_ + _)
      .map(tup => tup._1 + " : " + tup._2)
    distinctSubwayLines
  }

  private def getNameLengthRange(data: RDD[(String, String, String, String, String)]) = {
    val length = data.map(d => ("Name Length Range", (d._2.length, d._2.length)))
      .reduceByKey((d1, d2) => (if (d1._1 < d2._1) d1._1 else d2._1, if (d1._2 > d2._2) d1._2 else d2._2))
      .map(tup => tup._1 + " : " + tup._2.toString())
    length
  }

  private def getTotalCount(data: RDD[(String, String, String, String, String)]) = {
    val count = data.map(d => ("Count", 1))
      .reduceByKey(_ + _)
      .map(tup => tup._1 + " : " + tup._2)
    count
  }
}