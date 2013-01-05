package com.ghik

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 05.01.13
 * Time: 17:40
 */
class NullSinkFactory extends EventSinkFactory {
  def createEventSink[T](name: String) = new EventSink[T] {
    def insert(event: Event[T]) {}
    def clear() {}
    def flush() {}
  }
}
