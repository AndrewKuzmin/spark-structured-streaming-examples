package com.phylosoft.spark.learning.utils

import scala.reflect.runtime.universe._

/**
  * Created by Andrew on 5/20/2018.
  */


/**
  * Abstract class for parameter case classes.
  * This overrides the [[toString]] method to print all case class fields by name and value.
  *
  * @tparam T Concrete parameter class.
  */
abstract class AbstractParams[T: TypeTag] {

  private def tag: TypeTag[T] = typeTag[T]

  /**
    * Finds all case class fields in concrete class instance, and outputs them in JSON-style format:
    * {
    * [field name]:\t[field value]\n
    * [field name]:\t[field value]\n
    * ...
    * }
    */
  override def toString: String = {
    val tpe = tag.tpe
    val allAccessors = tpe.decls.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    val mirror = runtimeMirror(getClass.getClassLoader)
    val instanceMirror = mirror.reflect(this)
    allAccessors.map { f =>
      val paramName = f.name.toString
      val fieldMirror = instanceMirror.reflectField(f)
      val paramValue = fieldMirror.get
      s"  $paramName:\t$paramValue"
    }.mkString("{\n", ",\n", "\n}")
  }

}
