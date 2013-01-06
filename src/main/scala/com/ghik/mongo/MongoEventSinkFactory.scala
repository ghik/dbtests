package com.ghik.mongo

import com.ghik.EventSinkFactory
import com.mongodb.{WriteConcern, DBCollection, BasicDBObject, Mongo}
import scala.reflect.runtime.universe._

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 23:03
 */
class MongoEventSinkFactory(batchSize: Int, writeConcern: WriteConcern) extends EventSinkFactory {
  private val mongo = new Mongo
  mongo.setWriteConcern(writeConcern)

  def createEventSink[T: TypeTag](name: String) = {
    val coll: DBCollection = mongo.getDB(name).getCollection(name)

    import MongoEventSink._
    coll.ensureIndex(new BasicDBObject(DEVICE_ID_FIELD, 1).append(TIMESTAMP_FIELD, 1))
    coll.ensureIndex(new BasicDBObject(TIMESTAMP_FIELD, 1))

    new MongoEventSink[T](coll, batchSize)
  }
}
