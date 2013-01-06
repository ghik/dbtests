package com.ghik

import mongo.MongoEventSinkFactory
import mysql.{Engine, MysqlEventSinkFactory}
import com.mongodb.WriteConcern

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
    List(10000, 100000, 200000) foreach {
      batchSize =>
        runTest(new MongoEventSinkFactory(batchSize, WriteConcern.NORMAL), 100000, 1)
        runTest(new MongoEventSinkFactory(batchSize, WriteConcern.SAFE), 100000, 1)
        runTest(new MongoEventSinkFactory(batchSize, WriteConcern.JOURNAL_SAFE), 100000, 1)
        runTest(new MongoEventSinkFactory(batchSize, WriteConcern.FSYNC_SAFE), 100000, 1)
        runTest(new MysqlEventSinkFactory(batchSize, Engine.MyISAM), 100000, 1)
        runTest(new MysqlEventSinkFactory(batchSize, Engine.InnoDB), 100000, 1)
    }
  }

  def runTest(sinkFactory: EventSinkFactory, iters: Int, concurrencyLevel: Int) {
    List("numbers", "vectors", "strings") map sinkFactory.createEventSink foreach {_.clear()}

    def intSink = sinkFactory.createEventSink[Int]("numbers")
    def vecSink = sinkFactory.createEventSink[(Double, Double)]("vectors")
    def stringSink = sinkFactory.createEventSink[String]("strings")

    def configs = List[EventGeneratorConfig[_]](
      EventGeneratorConfig(intSink, r => r.nextInt(20000), 3),
      EventGeneratorConfig(vecSink, r => (r.nextDouble(), r.nextDouble()), 3),
      EventGeneratorConfig(stringSink, r => 20 * r.nextPrintableChar().toString, 3)
    )

    val eg = Traversable.fill(concurrencyLevel)(new EventGenerator(deviceCount, iters, configs))
    val ((), time) = benchmark {eg.par.foreach(_.run())}

    val totalEvents = eg.map(_.totalEvents).sum
    println("Took %s to insert %s events which is %s per sec.".format(time, totalEvents, totalEvents / time))
  }

  private def benchmark[T](expr: => T): (T, Double) = {
    val start = System.nanoTime()
    val res = expr
    (res, (System.nanoTime() - start) / 1000000000.0)
  }
}