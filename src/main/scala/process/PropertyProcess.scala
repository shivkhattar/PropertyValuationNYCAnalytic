package process

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import util.CommonConstants.{SPLIT_REGEX, BOROUGH_BLOCK, PROP_CURMKTTOT}

object PropertyProcess {
  def process(sc: SparkContext, hdfs: FileSystem, sess:SparkSession, cleanedPropertyPath: String): Unit = {
    import sess.implicits._
    val propertyData = sc.textFile(cleanedPropertyPath).map(_.split(SPLIT_REGEX))
      .map(x => (x(3) + "_" + x(4), x(2).toDouble))
      .groupByKey()
      .map(row => Map(BOROUGH_BLOCK -> row._1, PROP_CURMKTTOT -> row._2.to[Seq].toList.toString()))

    val columns = propertyData.take(1).flatMap(a=>a.keys)

    val resultantDF = propertyData.map{value=>
      val list=value.values.toList
      (list(0),list(1))
    }.toDF(columns:_*)

    val finalDF = resultantDF.map(row => (row.get(0), removeOutliers(row.get(1)))).toDF(BOROUGH_BLOCK, PROP_CURMKTTOT)

    //propertyData.take(10).foreach(println)
    //propertyData
    //propertyData.map(x => removeOutliers(sess, sc, x))
  }


  def removeOutliers(sess: SparkSession, sc: SparkContext, inputStr: String): Double = {
    val inputParsed = inputStr.substring(inputStr.indexOf('(')+1, inputStr.indexOf(')')).split(SPLIT_REGEX).map(s => s.toDouble).toList

    val schema = StructType(Array(StructField("value",DoubleType)))
    //val df = sess.createDataFrame(rowRDD,schema)
    val quantiles = df.stat.approxQuantile("value", Array(0.25,0.75),0.0)
    val Q1 = quantiles(0)
    val Q3 = quantiles(1)
    val IQR = Q3 - Q1
    val lowerRange = Q1 - 1.5*IQR
    val upperRange = Q3+ 1.5*IQR
    val outliersRemoved = df.filter(s"value >= $lowerRange and value < $upperRange")
    outliersRemoved.rdd.map(_.toSeq.toList).map(_.toString().toDouble).collect().toList
  }

  def meanElements(list: List[Double]): Double = list.sum / list.length
}
