package process

import org.apache.spark.SparkContext
import util.CommonConstants.{SPLIT_REGEX}

object PropertyProcess {
  def process(sc: SparkContext, cleanedPropertyPath: String): Unit = {
    val propertyData = sc.textFile(cleanedPropertyPath).map(_.split(SPLIT_REGEX))
      .map(x => (x(3) + "_" + x(4), x(2).toDouble))
      .groupByKey()
      .map(row => (row._1, row._2.to[Seq].toList))
      .map(r => (r._1, removeOutliers(r._2)))

    println(propertyData.count())
  }


  def removeOutliers(inputList: List[Double]): Double = {
    val size = inputList.size
    val sortedList = inputList.sorted
    val medianIndex = getMedianIndex(0, size)
    val Q1 = sortedList(getMedianIndex(0, medianIndex))
    val Q3 = sortedList(math.min(getMedianIndex(medianIndex + 1, size), size - 1))
    val IQR = Q3 - Q1
    val lowerRange = Q1 - 1.5 * IQR
    val upperRange = Q3 + 1.5 * IQR
    val outliersRemoved = sortedList.filter(prop_price => prop_price >= lowerRange && prop_price < upperRange)
    val averagePropertyValue = meanElements(outliersRemoved)
    averagePropertyValue
  }

  def getMedianIndex(left: Int, right: Int): Int = {
    var n = right - left + 1
    n = (n + 1) / 2 - 1
    n + left
  }

  def meanElements(list: List[Double]): Double = list.sum / list.length
}