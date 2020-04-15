package profile

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonUtil
import util.CommonUtil.getCountsGroupedByKeyForField
import util.CommonConstants._

object PropertyProfile extends Profile {

  def profile(sc: SparkContext, hdfs: FileSystem, originalInputPath: String, inputPath: String, outputPath: String): Unit = {
    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    CommonUtil.writeOriginalCount(sc, originalInputPath, outputPath)
    val data = sc.textFile(inputPath)
      .map(_.split(SPLIT_REGEX))
      .map(x => Map(PROP_PARID -> x(0), PROP_ZIPCODE -> x(1), PROP_CURMKTTOT -> x(2), PROP_BORO -> x(3), PROP_BLOCK -> x(4)
        , PROP_LOT -> x(5), PROP_ZONING -> x(6), PROP_YRBUILT -> x(7)))

    val count = CommonUtil.getTotalCount(data)
    count.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_PARID))

    val dates = getCountsGroupedByYearBuilt(data)
    dates.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_YRBUILT))

    val boroughs = getCountsGroupedByKeyForField(data, PROP_BORO)
    boroughs.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_BORO))

    val offenses = getCountsGroupedByKeyForField(data, PROP_ZONING)
    offenses.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_ZONING))

    val levels = getCountsGroupedByKeyForField(data, PROP_ZIPCODE)
    levels.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_ZIPCODE))

    val max = (getMaxPropertyVal(data))
    val min = (getMinPropertyVal(data))
    val max_min = sc.parallelize(Seq(max.toString + " " + min.toString))
    max_min.saveAsTextFile(outputPath + PROP_PROFILE_PATHS(PROP_MAX))
  }

  private def getCountsGroupedByYearBuilt(data: RDD[Map[String, String]]) = {
    data.map(row => row(PROP_YRBUILT))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(tup => tup._1 + PROFILER_SEPARATOR + tup._2.toString())
  }

  private def getMaxPropertyVal(data: RDD[Map[String, String]]): Long = {
    data.map(row => {println(row(PROP_CURMKTTOT));
                      (row(PROP_CURMKTTOT)).toLong
    }).max
  }

  private def getMinPropertyVal(data: RDD[Map[String, String]]): Long = {
    data.map(row => row(PROP_CURMKTTOT).toLong).min
  }
}