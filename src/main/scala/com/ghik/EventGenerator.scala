package com.ghik

import util.Random
import EventGenerator._

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 02.01.13
 * Time: 23:25
 */
class EventGenerator(deviceCount: Int, iters: Int, configs: Traversable[Config[_]]) extends Runnable {

  import utils._

  def run() {
    val r = new Random

    iters times {
      configs foreach {
        case (sink, dataGen, count) =>
          count times {
            val event = Event(r.nextInt(deviceCount), System.currentTimeMillis + 1000000, dataGen(r))
            sink.insert(event)
          }
      }
    }

    configs foreach {_._1.flush()}
  }

  val totalEvents = iters * configs.map(_._3).sum
}

object EventGenerator {
  type Config[T] = (EventSink[T], Random => T, Int)
}
