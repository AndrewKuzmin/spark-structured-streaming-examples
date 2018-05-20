package com.phylosoft.spark.learning.utils

import com.typesafe.config.Config

/**
  * Created by Andrew on 5/20/2018.
  */
object Provider {

  import com.typesafe.config.ConfigFactory

  private val config = ConfigFactory.load

  def getConfig: Config = config

}
