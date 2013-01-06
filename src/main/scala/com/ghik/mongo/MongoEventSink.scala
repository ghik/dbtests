package com.ghik.mongo

import scala.collection.JavaConversions._
import com.ghik.{Event, EventSink}
import com.mongodb.{DBCollection, DBObject, BasicDBObject, Mongo}
import java.util.Date
import collection.mutable.ArrayBuffer
import java.util
import com.google.common.collect.{Maps, Lists}

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 18:34
 */
class MongoEventSink[T](
                         coll: DBCollection,
                         batchSize: Int)
  extends EventSink[T] {

  import MongoEventSink._

  private var batch = new ArrayBuffer[DBObject](batchSize)

  def insert(event: Event[T]) {
    batch += toDocument[T](event)

    if (batch.size >= batchSize) {
      flush()
    }
  }


  def flush() {
    coll.insert(batch: java.util.List[DBObject])
    batch = new ArrayBuffer[DBObject](batchSize)
  }

  def clear() {
    coll.getDB.dropDatabase()
  }
}

object MongoEventSink {
  val DEVICE_ID_FIELD = "deviceId"
  val TIMESTAMP_FIELD = "timestamp"
  val DATA_FIELD = "data"

  private def toDocument[T](event: Event[T]): DBObject = {
    val res = new BasicDBObject
    res.put(DEVICE_ID_FIELD, event.deviceId)
    res.put(TIMESTAMP_FIELD, new Date(event.timestamp))
    res.put(DATA_FIELD, toMongo(event.data))
    res
  }

  private def toMongo(obj: Any): Any = obj match {
    case obj: Seq[_] => Lists.newArrayList(obj.iterator: java.util.Iterator[_])
    case obj: Product => Lists.newArrayList(obj.productIterator: java.util.Iterator[_])
    case obj: Map[_,_] =>
      def pairToMongo(pair: (Any, Any)): (Any, Any) = pair match {
        case (f, s) => (toMongo(f), toMongo(s))
      }
      Maps.newHashMap[Any, Any](mapAsJavaMap(obj.asInstanceOf[Map[Any, Any]].map(pairToMongo)))
    case _ => obj
  }

  def main(args: Array[String]) {
    println(toMongo((1,2,3)))
  }
}