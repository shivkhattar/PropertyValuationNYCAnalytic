package clean

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import util.CommonUtil
import util.CommonConstants._

object PropertyClean extends Clean {

  def clean(sc: SparkContext, hdfs: FileSystem, inputPath: String, outputPath: String): Unit = {

    val data = sc.textFile(inputPath)
      .filter(!_.startsWith(PROP_PARID)).map(d => if (d.endsWith(",")) d.concat(UNKNOWN) else d)

    val columnsRemoved = data.map(_.split(SPLIT_REGEX))
      .filter(x => x.length == 139 && x(7).equals("2020"))
      .map(x => Map(PROP_PARID -> x(0), PROP_BORO -> x(1), PROP_BLOCK -> x(2), PROP_LOT -> x(3), PROP_EASEMENT -> x(4), PROP_ZIPCODE -> x(77), PROP_YRBUILT -> x(90), PROP_ZONING -> x(73),
        PROP_CURMKTTOT -> x(57)))

    val cleanedProperty = columnsRemoved.filter(row => !row(PROP_PARID).isEmpty && !row(PROP_ZIPCODE).isEmpty && !row(PROP_CURMKTTOT).isEmpty && !row(PROP_BORO).isEmpty
      && !row(PROP_BLOCK).isEmpty && !row(PROP_LOT).isEmpty && !row(PROP_CURMKTTOT).equals("0"))
    val tupledProp = cleanedProperty.map(row => (row(PROP_PARID), row(PROP_ZIPCODE), row(PROP_CURMKTTOT), row(PROP_BORO), row(PROP_BLOCK), row(PROP_LOT), row(PROP_ZONING), row(PROP_YRBUILT)))
      .map(tup => tup.toString.substring(1, tup.toString.length - 1))

    CommonUtil.deleteFolderIfAlreadyExists(hdfs, outputPath)
    tupledProp.saveAsTextFile(outputPath)
  }
}
