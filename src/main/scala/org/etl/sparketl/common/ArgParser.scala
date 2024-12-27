package org.etl.sparketl.common

import org.rogach.scallop.{ScallopConf, ScallopOption}

class ArgParser(arguments: Array[String]) extends ScallopConf(arguments) {

  val configFile: ScallopOption[String] = opt[String](Constant.CONFIGFILE, required = true)
  val accessKey: ScallopOption[String] = opt[String](Constant.ACCESSKEY, default = Some(Constant.DEFAULT))
  val secreteKey: ScallopOption[String] = opt[String](Constant.SECRETKEY, default = Some(Constant.DEFAULT))
  val region: ScallopOption[String] = opt[String](Constant.REGION, default = Some(Constant.DEFAULT))
  val log4jFile: ScallopOption[String] = opt[String](Constant.LOG4J, default = Some(Constant.DEFAULT))


  verify()
}


/*
def main(args: Array[String]): Unit = {
  val conf = new Conf(args)
  println("high is: " + conf.high())
  println("low is: " + conf.low())
  println("name is: " + conf.name())
}*/
