package com.ghik.berkeleydb.je

import com.ghik.{EventSink, EventSinkFactory}
import scala.reflect.runtime.universe._
import com.sleepycat.je.{DatabaseConfig, Environment, EnvironmentConfig}
import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 21.01.13
 * Time: 22:06
 */
class BerkeleyDBEventSinkFactory(path: File) extends EventSinkFactory {
  private val envConf = new EnvironmentConfig
  envConf.setAllowCreate(true)

  val dbEnv = new Environment(path, envConf)

  def createEventSink[T: TypeTag](name: String): EventSink[T] = {
    val dbConf = new DatabaseConfig
    dbConf.setAllowCreate(true)

    new BerkeleyDBEventSink[T](dbEnv, dbEnv.openDatabase(null, name, dbConf))
  }

  val description: String = "BerkeleyDB Java Edition"
}
