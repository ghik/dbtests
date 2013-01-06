package com.ghik

import mongo.MongoEventSinkFactory
import sql.mysql.{Engine, MySQLEventSinkFactory}
import collection.mutable.ArrayBuffer
import com.mongodb.WriteConcern
import sql.postgresql.PostgreSQLEventSinkFactory

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 18:11
 */
object TestRunner {

  import utils._

  val deviceCount = 100
  val iters = 1000000

  def main(args: Array[String]) {
    List(10000, 100000, 200000) foreach {
      batchSize =>
        runTest(new MongoEventSinkFactory(batchSize, WriteConcern.NORMAL), iters, 1)
        runTest(new MySQLEventSinkFactory(batchSize, Engine.MyISAM), iters, 1)
        runTest(new MySQLEventSinkFactory(batchSize, Engine.InnoDB), iters, 1)
        runTest(new PostgreSQLEventSinkFactory(batchSize), iters, 1)
    }
  }

  def runTest(sinkFactory: EventSinkFactory, iters: Int, concurrencyLevel: Int) {
    def intSinks = (1 to 1).map("numbers" + _).map(sinkFactory.createEventSink[Int])
    def vecSinks = (1 to 0).map("vectors" + _).map(sinkFactory.createEventSink[(Double, Double)])
    def stringSinks = (1 to 0).map("strings" + _).map(sinkFactory.createEventSink[String])

    def sinks = ArrayBuffer() ++ intSinks ++ vecSinks ++ stringSinks
    sinks foreach {_.clear()}

    def configs = ArrayBuffer[EventGeneratorConfig[_]]() ++
      intSinks.map {EventGeneratorConfig(_, r => r.nextInt(20000), 1)} ++
      vecSinks.map {EventGeneratorConfig(_, r => (r.nextDouble(), r.nextDouble()), 1)} ++
      stringSinks.map {EventGeneratorConfig(_, r => 20 * r.nextPrintableChar().toString, 1)}

    val eg = Traversable.fill(concurrencyLevel)(new EventGenerator(deviceCount, iters, configs))
    val ((), time) = benchmark {eg.par.foreach(_.run())}

    val totalEvents = eg.map(_.totalEvents).sum
    println("Took %s to insert %s events which is %s per sec.".format(time, totalEvents, totalEvents / time))

    Thread.sleep(30000)
  }

}