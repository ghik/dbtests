package com.ghik

import util.Random

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 02.01.13
 * Time: 23:25
 */
class EventGenerator(deviceCount: Int, iters: Int, configs: Traversable[EventGeneratorConfig[Any]]) extends Runnable {

  import utils._

  def run() {
    val r = new Random

    iters times {
      configs foreach {
        case EventGeneratorConfig(sink, dataGen, count) =>
          count times {
            val event: Event[Any] = Event(r.nextInt(deviceCount), System.currentTimeMillis, dataGen(r))
            sink.insert(event)
          }
      }
    }

    configs foreach {_.sink.flush()}
  }

  val totalEvents = iters * configs.map(_.count).sum
}
