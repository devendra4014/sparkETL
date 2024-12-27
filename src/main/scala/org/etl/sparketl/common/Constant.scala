package org.etl.sparketl.common

object Constant {

  final val CONFIGFILE: String = "configFile"
  final val ACCESSKEY: String = "accessKey"
  final val SECRETKEY: String = "secreteKey"
  final val REGION: String = "region"
  final val LOG4J: String = "log4j"
  final val DEFAULT: String = ""
  final val BUCKET: String = "bucket"
  final val PATH: String = "path"
  final val FORMAT: String = "format"
  final val DELIMITER: String = "delimiter"
  final val OBJECT: String = "object"
  final val HEADER: String = "header"
  final val TRUE: String = "true"
  final val FALSE: String = "false"
  final val MODE: String = "mode"
  final val MULTILINE: String = "multiLine"
  final val PERMISSIVE: String = "PERMISSIVE"

  // Format Constants
  final val CSV: String = "csv"
  final val PARQUET: String = "parquet"
  final val ORC: String = "orc"
  final val JSON: String = "json"

  //
  final val ACCESS_KEY_CONF: String = "fs.s3a.access.key"
  final val SECRET_KEY_CONF: String = "fs.s3a.secret.key"
  final val S3_END_POINT_CONF: String = "fs.s3a.endpoint"
  final val S3_END_POINT: String = "s3.amazonaws.com"

  //
  final val OVERWRITE: String = "overwrite"
  final val UPDATE: String = "update"
  final val COMPLETE: String = "overwrite"
  final val APPEND: String = "append"
  final val IGNORE: String = "ignore"

}
