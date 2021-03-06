package com.ghik.sql.mysql

import java.sql._
import collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._
import com.ghik.{utils, EventSink, Event}

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 06.01.13
 * Time: 17:33
 */
class MySQLEventSink[T: TypeTag](name: String, batchSize: Int, conn: Connection) extends EventSink[T] {
  import utils._

  private var batch = new ArrayBuffer[Event[T]](batchSize)

  def insert(event: Event[T]) {
    batch += event

    if (batch.size >= batchSize) {
      flush()
    }
  }

  def flush() {
    if(batch.nonEmpty) {
      val sqlb = new StringBuilder("insert into %s (deviceId, tstamp, data) values ".format(name))

      (batch.size - 1) times sqlb.append("(?,?,?),")
      sqlb.append("(?,?,?);")

      val st = conn.prepareStatement(sqlb.toString())
      batch.view.zipWithIndex foreach {
        case (Event(deviceId, timestamp, data), i) =>
          st.setInt(3 * i + 1, deviceId)
          st.setLong(3 * i + 2, timestamp)
          updateStatement(st, 3 * i + 3, data)
      }

      st.executeUpdate()
      st.close()

      batch = new ArrayBuffer[Event[T]](batchSize)
    }
  }

  private def updateStatement(st: PreparedStatement, index: Int, data: T) {
    typeOf[T] match {
      case t if t =:= typeOf[Int] => st.setInt(index, data.asInstanceOf[Int])
      case _ => st.setString(index, data.toString)
    }
  }

  def clear() {
    val st = conn.prepareStatement("drop table %s;".format(name))
    st.executeUpdate()
    st.close()
  }
}
