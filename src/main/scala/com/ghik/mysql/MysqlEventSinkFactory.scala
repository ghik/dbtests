package com.ghik.mysql

import com.ghik.{EventSink, EventSinkFactory}
import java.sql.DriverManager
import scala.reflect.runtime.universe._
import MysqlEventSinkFactory._

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 05.01.13
 * Time: 22:08
 */
class MysqlEventSinkFactory(batchSize: Int, engine: Engine) extends EventSinkFactory {
  val stringClass = classOf[String]

  def createEventSink[T: TypeTag](name: String): EventSink[T] = {
    val jdbcUrl: String = "jdbc:mysql://localhost/dbtests?useServerPrepStmts=false&rewriteBatchedStatements=true"
    val conn = DriverManager.getConnection(jdbcUrl, "root", "")

    val st = conn.prepareStatement(
      """create table if not exists %s (
             id int not null auto_increment primary key,
             deviceId int not null,
             tstamp timestamp not null,
             data %s,
             index (deviceId),
             index (tstamp, deviceId)
         )
         engine = %s
      """.format(name, sqlType[T], engine)
    )

    st.executeUpdate()
    st.close()

    new MysqlEventSink[T](name, batchSize, conn)
  }
}

object MysqlEventSinkFactory {

  private def sqlType[T: TypeTag]: String = typeOf[T] match {
    case t if t =:= typeOf[Int] => "int"
    case _ => "varchar(255)"
  }

  def main(args: Array[String]) {
    new MysqlEventSinkFactory(100, Engine.MyISAM).createEventSink[Int]("stuff")
  }
}