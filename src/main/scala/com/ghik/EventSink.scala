package com.ghik

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 18:29
 * To change this template use File | Settings | File Templates.
 */
trait EventSink[T] {
  def insert(event: Event[T])
  def flush()
  def clear()
}
