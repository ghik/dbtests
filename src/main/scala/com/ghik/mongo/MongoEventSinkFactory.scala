package com.ghik.mongo

import com.ghik.EventSinkFactory
import com.mongodb.Mongo

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 23:03
 * To change this template use File | Settings | File Templates.
 */
class MongoEventSinkFactory(batchSize: Int) extends EventSinkFactory {
  private val mongo = new Mongo

  def createEventSink[T](name: String) = new MongoEventSink[T](name, mongo, batchSize)
}
