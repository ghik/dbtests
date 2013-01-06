package com.ghik.sql.postgresql

import com.ghik.{EventSink, EventSinkFactory}
import java.sql.DriverManager
import scala.reflect.runtime.universe._
import PostgreSQLEventSinkFactory._

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 05.01.13
 * Time: 22:08
 */
class PostgreSQLEventSinkFactory(batchSize: Int) extends EventSinkFactory {
  def createEventSink[T: TypeTag](name: String): EventSink[T] = {
    val jdbcUrl: String = "jdbc:postgresql://localhost/dbtests"
    val conn = DriverManager.getConnection(jdbcUrl, "postgres", "")

    val st = conn.prepareStatement(
      """create table if not exists %s (
             id serial not null primary key,
             deviceId integer not null,
             tstamp bigint not null,
             data %s
         );
         create index on %s (deviceId);
         create index on %s (tstamp, deviceId);
      """.format(name, sqlType[T], name, name)
    )

    st.executeUpdate()
    st.close()

    new PostgreSQLEventSink[T](name, batchSize, conn)
  }

  val description = "PostgreSQL"
}

object PostgreSQLEventSinkFactory {
  private def sqlType[T: TypeTag]: String = typeOf[T] match {
    case t if t =:= typeOf[Int] => "integer"
    case _ => "varchar(255)"
  }

  def main(args: Array[String]) {
    new PostgreSQLEventSinkFactory(100).createEventSink[Int]("stuff")
  }
}