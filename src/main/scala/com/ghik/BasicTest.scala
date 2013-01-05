package com.ghik

import mongo.MongoEventSinkFactory

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 18:11
 */
object BasicTest {

  import utils._

  val deviceCount = 100

  def main(args: Array[String]) {
    (1 to 1) foreach {
      concurrencyLevel: Int => runTest(new MongoEventSinkFactory(50000), 30000 / concurrencyLevel, concurrencyLevel)
    }
  }

  def runTest(sinkFactory: EventSinkFactory, iters: Int, concurrencyLevel: Int) {
    List("numbers", "vectors", "strings") map sinkFactory.createEventSink foreach {_.clear()}

    def intSink = sinkFactory.createEventSink[Any]("numbers")
    def vecSink = sinkFactory.createEventSink[Any]("vectors")
    def stringSink = sinkFactory.createEventSink[Any]("strings")

    def configs = List[EventGeneratorConfig[Any]](
      EventGeneratorConfig(intSink, r => r.nextInt(20000), 3),
      EventGeneratorConfig(vecSink, r => (r.nextDouble(), r.nextDouble()), 3),
      EventGeneratorConfig(stringSink, r => 20 * r.nextPrintableChar().toString, 3)
    )

    val eg = Traversable.fill(concurrencyLevel)(new EventGenerator(deviceCount, iters, configs))
    val ((), time) = benchmark {eg.foreach(_.run())}

    val totalEvents = eg.map(_.totalEvents).sum
    println("Took %s to insert %s events which is %s per sec.".format(time, totalEvents, totalEvents / time))
  }

  private def benchmark[T](expr: => T): (T, Double) = {
    val start = System.nanoTime()
    val res = expr
    (res, (System.nanoTime() - start) / 1000000000.0)
  }
}