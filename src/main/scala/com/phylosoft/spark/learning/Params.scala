package com.phylosoft.spark.learning

import com.phylosoft.spark.learning.AppConfig.Mode
import com.phylosoft.spark.learning.utils.AbstractParams

/**
  * Created by Andrew on 5/20/2018.
  */
case class Params(environment: String = null,
                  mode: Mode.Mode = Mode.REALTIME,
                  inputPath: String = null,
                  outputPath: String = null)
  extends AbstractParams[Params]