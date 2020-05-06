package process

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.CommonConstants.SPLIT_REGEX

object PropertyProcess {
  //  Function to preprocess Property data by removing outlier values from each borough-block/ zip code
  //  in order to normalize the average property prices in the locality under analysis.
  def process(sc: SparkContext, cleanedPropertyPath: String): RDD[(String, Double)] = {
    sc.textFile(cleanedPropertyPath).map(_.split(SPLIT_REGEX))
      .map(x => (x(3) + "_" + x(4), x(2).toDouble / x(8).toDouble))
      .groupByKey()
      .map(row => (row._1, row._2.to[Seq].toList))
      .map(r => {
        if (r._2.length > 2) (r._1, removeOutliers(r._2))
        else (r._1, meanElements(r._2))
      })
      .filter(_._2 > 0.0)
  }

  //  Helper function to remove outliers based on Box and whisker plot, by keeping only those values between
  //  {25th percentile - 1.5 times Inter-quartile range} (IQR) and {75th percentile + 1.5 times IQR}
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
    meanElements(outliersRemoved)
  }

  //  Helper function to return the index of the median value in a given range of indices.
  def getMedianIndex(left: Int, right: Int): Int = {
    var n = right - left + 1
    n = (n + 1) / 2 - 1
    n + left
  }

  //  Helper function to return mean of a list.
  def meanElements(list: List[Double]): Double = {
    if (list.length == 0) 0.0
    else list.sum / list.length
  }
}
