package com.ghik.sql

import com.ghik.EventSink
import java.sql._
import collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._
import com.ghik.Event

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 05.01.13
 * Time: 22:09
 */
class SQLEventSink[T: TypeTag](name: String, batchSize: Int, conn: Connection) extends EventSink[T] {

  private var batch = new ArrayBuffer[Event[T]](batchSize)

  def insert(event: Event[T]) {
    batch += event

    if (batch.size >= batchSize) {
      flush()
    }
  }

  def flush() {
    if (batch.nonEmpty) {
      val sqlb = new StringBuilder("insert into %s (deviceId, tstamp, data) values ".format(name))

      batch.view.zipWithIndex foreach {
        case (Event(deviceId, timestamp, data), i) =>
          sqlb.append("(%d,%d,%s)".format(deviceId, timestamp, formatData(data)))
          sqlb.append(if (i < batch.size - 1) "," else ";")
      }

      val st = conn.prepareStatement(sqlb.toString())
      st.executeUpdate()
      st.close()

      batch = new ArrayBuffer[Event[T]](batchSize)
    }
  }

  private def formatData(data: T): String = {
    typeOf[T] match {
      case t if t =:= typeOf[Int] => String.valueOf(data)
      case _ => "\'" + data.toString() + "\'"
    }
  }

  def clear() {
    val st = conn.prepareStatement("drop table %s;".format(name))
    st.executeUpdate()
    st.close()
  }
}
