package com.ghik

import scala.reflect.runtime.universe._

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 23:03
 */
trait EventSinkFactory {
  def createEventSink[T: TypeTag](name: String): EventSink[T]
  val description: String
}
