package com.ghik.mongo

import scala.collection.JavaConversions._
import com.ghik.{Event, EventSink}
import com.mongodb.{DBObject, BasicDBObject, Mongo}
import java.util.Date
import collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 18:34
 * To change this template use File | Settings | File Templates.
 */
class MongoEventSink[T](
                         name: String,
                         mongo: Mongo,
                         batchSize: Int)
  extends EventSink[T] {

  private val db = mongo.getDB(name)
  private val coll = db.getCollection(name)
  coll.ensureIndex(new BasicDBObject("deviceId", 1).append("timestamp", 1))
  coll.ensureIndex(new BasicDBObject("timestamp", 1))

  private val batch = new ArrayBuffer[DBObject]

  def insert(event: Event[T]) {
    def tomongo(event: Event[T]) = {
      val res = new BasicDBObject
      res.put("deviceId", event.deviceId)
      res.put("timestamp", new Date(event.timestamp))
      res.put("data", event.data)
      res
    }

    batch += tomongo(event)

    if (batch.size >= batchSize) {
      flush()
    }
  }

  def flush() {
    coll.insert(batch: java.util.List[DBObject])
    batch.clear()
  }

  def clear() {
    db.dropDatabase()
  }
}
