package util

object CommonConstants {

  final val UNKNOWN = "UNKNOWN"

  final val SPLIT_REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

  final val CMPLNT_NUM = "CMPLNT_NUM"

  final val DATE = "DATE"

  final val OFFENSE_DESC = "OFFENSE_DESC"

  final val LEVEL = "LEVEL"

  final val BOROUGH = "BOROUGH"

  final val LATITUDE = "LATITUDE"

  final val LONGITUDE = "LONGITUDE"

  final val X_COORD = "X_COORD"

  final val Y_COORD = "Y_COORD"

  final val SUSPECT_AGE = "SUSPECT_AGE"

  final val SUSPECT_RACE = "SUSPECT_RACE"

  final val SUSPECT_SEX = "SUSPECT_SEX"

  final val OBJECT_ID = "OBJECT_ID"

  final val STATION_NAME = "STATION_NAME"

  final val LAT_LONG= "LATLONG"

  final val SUBWAY_LINE = "SUBWAY_LINE"

  final val SUBWAY_LINE_SEPERATOR = "-"

  final val URL = "URL"

  final val LAT_LONG_PREFIX = "POINT ("

  final val LAT_LONG_SUFFIX = ")"

  final val LAT_LONG_SEPARATOR = " "

  final val PROFILER_SEPARATOR = " : "

  final val CRIME_PROFILE_PATHS = Map(CMPLNT_NUM -> "/count", DATE -> "/dates", OFFENSE_DESC -> "/offense_descs", LEVEL -> "/levels", BOROUGH -> "/boroughs",
    SUSPECT_AGE -> "/suspect_age", SUSPECT_RACE -> "/suspect_race", SUSPECT_SEX -> "/suspect_sex")

  final val SUBWAY_PROFILE_PATHS = Map(OBJECT_ID -> "/count", SUBWAY_LINE -> "/countOfSubwayLines")

  final val DISTINCT_SUBWAY_LINES_KEY = "Distinct Subway Lines"
  final val NAME_LENGTH_RANGE_KEY = "Name Length Range"
  final val COUNT_KEY = "Count"


}