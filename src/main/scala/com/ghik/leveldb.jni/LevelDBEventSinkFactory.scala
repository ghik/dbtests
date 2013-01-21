package com.ghik.leveldb

import com.ghik.{EventSink, EventSinkFactory}
import scala.reflect.runtime.universe._
import java.io.File
import org.iq80.leveldb.Options

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 20.01.13
 * Time: 14:57
 */
class LevelDBEventSinkFactory(dbpath: File, batchSize: Int) extends EventSinkFactory {

  private[this] val options = new Options().createIfMissing(true)

  def createEventSink[T: TypeTag](name: String): EventSink[T] = {
    new LevelDBEventSink[T](new File(dbpath, name), options, batchSize)
  }

  val description: String = "LevelDB"
}
