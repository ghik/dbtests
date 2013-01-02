package com.ghik

import mongo.{MongoEventSinkFactory, MongoEventSink}
import collection.mutable.ArrayBuffer
import util.Random

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 18:11
 * To change this template use File | Settings | File Templates.
 */
object BasicTest {
  val eventCount = 3000000
  val batchSize = 100000
  val deviceCount = 100

  val sinkFactory: EventSinkFactory = new MongoEventSinkFactory(batchSize)

  def main(args: Array[String]) {
    val r = new Random

    val sink = sinkFactory.createEventSink[Int]("eventz")
    sink.clear()

    val (_, time) = benchmark {
      var i = 0
      if(i % 10000 == 0) {
        println("already "+i)
      }
      while (i < eventCount) {
        sink.insert(Event(r.nextInt(deviceCount), System.currentTimeMillis(), r.nextInt(20000)))
        i += 1
      }
      sink.flush()
    }

    val secs = time / 1000000000.0
    println("Took " + secs + " which is " + eventCount / secs + " per sec.")
  }

  private def benchmark[T](expr: => T): (T, Long) = {
    val start = System.nanoTime()
    val res = expr
    (res, System.nanoTime() - start)
  }
}