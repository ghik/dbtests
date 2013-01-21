package com.ghik.leveldb

import com.ghik.{Event, EventSink}
import org.iq80.leveldb.{WriteBatch, Options, DB}
import org.fusesource.leveldbjni.JniDBFactory
import java.io.File
import java.nio.ByteBuffer
import scala.reflect.runtime.universe._

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 20.01.13
 * Time: 14:59
 */

object LevelDBEventSink {
  private val UTF8_CHARSET = "UTF-8"
}

import LevelDBEventSink._

class LevelDBEventSink[T](path: File, options: Options, batchSize: Int) extends EventSink[T] {

  import JniDBFactory._

  private[this] val db = factory.open(path, options)
  private[this] var (batch, size): (WriteBatch, Int) = (db.createWriteBatch, 0)

  def insert(event: Event[T]) {
    batch.put(serializeKey(event.timestamp, event.deviceId), serialize(event.data))
    size += 1

    if (size >= batchSize) {
      flush()
    }
  }

  def flush() {
    db.write(batch)
    batch = db.createWriteBatch
  }

  def clear() {
    factory.destroy(path, options)
    path.delete()
  }

  private def serializeKey(tstamp: Long, devid: Int): Array[Byte] =
    ByteBuffer.allocate(12).putLong(tstamp).putInt(devid).array

  private def serialize(obj: Any): Array[Byte] = obj match {
    case obj: Int => ByteBuffer.allocate(4).putInt(obj).array
    case obj: Long => ByteBuffer.allocate(8).putLong(obj).array
    case obj: Float => ByteBuffer.allocate(4).putFloat(obj).array
    case obj: Double => ByteBuffer.allocate(8).putDouble(obj).array
    case obj: String => obj.getBytes(UTF8_CHARSET)
    case obj: (_, _) => serialize(obj._1) ++ serialize(obj._2)
  }
}


