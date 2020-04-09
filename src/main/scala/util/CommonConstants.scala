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

  final val CRIME_PROFILE_PATHS = Map(CMPLNT_NUM -> "/count", DATE -> "/dates", OFFENSE_DESC -> "/offense_descs", LEVEL -> "/levels", BOROUGH -> "/boroughs",
    SUSPECT_AGE -> "/suspect_age", SUSPECT_RACE -> "/suspect_race", SUSPECT_SEX -> "/suspect_sex")

}
