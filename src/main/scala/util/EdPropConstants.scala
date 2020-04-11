package util

object EdPropConstants {

  val EDUCATION_PATH = "/education.csv"

  val PROPERTY_PATH = "/property.csv"

  val CLEANED_EDUCATION_PATH = "/output/cleaned/education"

  val CLEANED_PROPERTY_PATH = "/output/cleaned/property"

  val PROFILED_EDUCATION_PATH = "/output/profiled/education"

  val PROFILED_PROPERTY_PATH = "/output/profiled/property"

  final val FILE_SEPARATOR = "/"

  final val SPLIT_REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

  final val UNKNOWN = "UNKNOWN"

  final val PROP_BORO = "BORO"

  final val PROP_BLOCK = "BLOCK"

  final val PROP_LOT = "LOT"

  final val PROP_EASEMENT = "EASEMENT"

  final val PROP_PARID = "PARID"

  final val PROP_CURMKTTOT = "CURMKTTOT"

  final val PROP_ZIPCODE = "ZIP_CODE"

  final val PROP_YRBUILT = "YRBUILT"

  final val PROP_BLDG_CLASS = "BLDG_CLASS"

  final val PROP_ZONING = "ZONING"

  final val ED_ATS_SYSTEM_CODE = "ATS_SYSTEM_CODE"

  final val ED_LOCATION_NAME = "LOCATION_NAME"

  final val ED_BBL = "BOROUGH_BLOCK_LOT"

  final val ED_GRADES_FINAL_TEXT = "GRADES_FINAL_TEXT"

  final val ED_OPEN_DATE = "OPEN_DATE"

  final val ED_LOCATION1 = "LOCATION1"

  final val ED_LAT_LONG_PREFIX = "("

  final val ED_LAT_LONG_SUFFIX = ")\""

  final val ED_ZIP_CODE = "ZIPCODE"

  final val ED_LATITUDE = "LATITUDE"

  final val ED_LONGITUDE = "LONGITUDE"

  final val ED_LAT_LONG_SEPARATOR = ","

  final val PROFILER_SEPARATOR = " : "

  final val PROP_MAX = "max"

  final val PROP_MIN = "min"

  final val PROP_PROFILE_PATHS = Map(PROP_PARID -> "/parid", PROP_ZIPCODE -> "/zipcode", PROP_CURMKTTOT -> "/current_mkt_value", PROP_BORO -> "/borough",
    PROP_BLOCK -> "/block", PROP_LOT -> "/lot", PROP_ZONING -> "/zone" ,PROP_YRBUILT -> "/year_built", PROP_MAX -> "/max", PROP_MIN -> "/min")

  final val ED_PROFILE_PATHS = Map(ED_ATS_SYSTEM_CODE -> "/ats_code", ED_ZIP_CODE -> "/zip_code", ED_OPEN_DATE -> "/open_year", ED_GRADES_FINAL_TEXT -> "/grades_taught"
    ,ED_BBL -> "/bbl")
}
