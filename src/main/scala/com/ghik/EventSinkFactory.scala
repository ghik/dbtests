package com.ghik

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 23:03
 * To change this template use File | Settings | File Templates.
 */
trait EventSinkFactory {
  def createEventSink[T](name: String): EventSink[T]
}