package util

object CommonConstants {

  final val CRIME_PATH = "/crime.csv"

  final val SUBWAY_PATH = "/subway.csv"

  final val PLUTO_PATH = "/pluto.csv"

  final val CLEANED_CRIME_PATH = "/output/cleaned/crime"

  final val CLEANED_SUBWAY_PATH = "/output/cleaned/subway"

  final val CLEANED_PLUTO_PATH = "/output/cleaned/pluto"

  final val PROFILE_CRIME_PATH = "/output/profiled/crime"

  final val PROFILE_SUBWAY_PATH = "/output/profiled/subway"

  final val PROFILE_PLUTO_PATH = "/output/profiled/pluto"

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

  final val LAT_LONG = "LATLONG"

  final val SUBWAY_LINE = "SUBWAY_LINE"

  final val BLOCK = "BLOCK"

  final val LOT = "LOT"

  final val ZIPCODE = "ZIPCODE"

  final val ADDRESS = "ADDRESS"

  final val BOROUGH_BLOCK = "BOROUGH_BLOCK"

  final val BOROCODE = "BOROCODE"

  final val BOROBLOCK = "BOROBLOCK"

  final val BBL = "BBL"

  final val LINE = "LINE"

  final val DISTINCT_SUBWAY_LINES = "DISTINCT_SUBWAY_LINES"

  final val SUBWAY_LINE_SEPERATOR = "-"

  final val URL = "URL"

  final val LAT_LONG_PREFIX = "POINT ("

  final val LAT_LONG_SUFFIX = ")"

  final val LAT_LONG_SEPARATOR = " "

  final val PROFILER_SEPARATOR = " : "

  final val FILE_SEPARATOR = "/"

  final val BOROUGH_MAP = Map("BX" -> "Bronx", "BK" -> "Brooklyn", "MN" -> "Manhattan", "QN" -> "Queens", "SI" -> "Staten Island")

  final val ORIGINAL_COUNT_PATH = "/originalCount"

  final val CRIME_PROFILE_PATHS = Map(CMPLNT_NUM -> "/count", DATE -> "/dates", OFFENSE_DESC -> "/offense_descs", LEVEL -> "/levels", BOROUGH -> "/boroughs",
    SUSPECT_AGE -> "/suspect_age", SUSPECT_RACE -> "/suspect_race", SUSPECT_SEX -> "/suspect_sex")

  final val SUBWAY_PROFILE_PATHS = Map(OBJECT_ID -> "/count", SUBWAY_LINE -> "/countOfSubwayLines", STATION_NAME -> "/stationNameRange", DISTINCT_SUBWAY_LINES -> "/distinctSubwayLines")

  final val PLUTO_PROFILE_PATHS = Map(BBL -> "/count", BOROUGH -> "/boroughs", ZIPCODE -> "/zipcodes", ADDRESS -> "/addressLengthRange")

  final val DISTINCT_SUBWAY_LINES_KEY = "Distinct Subway Lines"

  final val NAME_LENGTH_RANGE_KEY = "Name Length Range"

  final val ADDRESS_LENGTH_RANGE_KEY = "Address Length Range"

  final val COUNT_KEY = "Count"

  final val EDUCATION_PATH = "/education.csv"

  final val PROPERTY_PATH = "/property.csv"

  final val CLEANED_EDUCATION_PATH = "/output/cleaned/education"

  final val CLEANED_PROPERTY_PATH = "/output/cleaned/property"

  final val PROFILED_EDUCATION_PATH = "/output/profiled/education"

  final val PROFILED_PROPERTY_PATH = "/output/profiled/property"

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

  final val PROP_MAX = "max"

  final val PROP_MIN = "min"

  final val PROP_PROFILE_PATHS = Map(PROP_PARID -> "/parid", PROP_ZIPCODE -> "/zipcode", PROP_CURMKTTOT -> "/current_mkt_value", PROP_BORO -> "/borough",
    PROP_BLOCK -> "/block", PROP_LOT -> "/lot", PROP_ZONING -> "/zone", PROP_YRBUILT -> "/year_built", PROP_MAX -> "/max", PROP_MIN -> "/min")

  final val ED_PROFILE_PATHS = Map(ED_ATS_SYSTEM_CODE -> "/ats_code", ED_ZIP_CODE -> "/zip_code", ED_OPEN_DATE -> "/open_year", ED_GRADES_FINAL_TEXT -> "/grades_taught"
    , ED_BBL -> "/bbl")

  final val RADIUS_OF_EARTH_IN_KM: Double = 6371
}