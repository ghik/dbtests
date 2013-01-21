package com.ghik.berkeleydb.je

import com.ghik.{Event, EventSink}
import com.sleepycat.je.{DatabaseEntry, Environment, Database}
import scala.reflect.runtime.universe._
import com.sleepycat.bind.tuple._
import com.ghik.Event

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 21.01.13
 * Time: 22:50
 */
class BerkeleyDBEventSink[T: TypeTag](dbEnv: Environment, db: Database) extends EventSink[T] {
  def insert(event: Event[T]) {
    val key = setupEntry(TupleBase.outputToEntry(new TupleOutput().writeLong(event.timestamp).writeInt(event.deviceId), _))
    val value = toEntry(event.data)
    db.put(null, key, value)
  }

  def flush() {}

  def clear() {
    val name = db.getDatabaseName
    db.close()
    dbEnv.removeDatabase(null, name)
  }

  private def setupEntry(func: DatabaseEntry => Any): DatabaseEntry = {
    val entry = new DatabaseEntry
    func(entry)
    entry
  }

  private val toEntry: T => DatabaseEntry = (typeOf[T] match {
    case t if t =:= typeOf[Int] => {data: Int => setupEntry(IntegerBinding.intToEntry(data, _))}
    case t if t =:= typeOf[Long] => {data: Long => setupEntry(LongBinding.longToEntry(data, _))}
    case t if t =:= typeOf[Float] => {data: Float => setupEntry(FloatBinding.floatToEntry(data, _))}
    case t if t =:= typeOf[Double] => {data: Double => setupEntry(DoubleBinding.doubleToEntry(data, _))}
    case t if t =:= typeOf[String] => {data: String => setupEntry(StringBinding.stringToEntry(data, _))}
    case t if t =:= typeOf[(Double,Double)] => {data: (Double,Double) =>
      setupEntry(TupleBase.outputToEntry(new TupleOutput().writeDouble(data._1).writeDouble(data._2), _))
    }
  }).asInstanceOf[T => DatabaseEntry]
}
