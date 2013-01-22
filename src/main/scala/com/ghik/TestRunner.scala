package com.ghik

import berkeleydb.je.BerkeleyDBEventSinkFactory
import leveldb.LevelDBEventSinkFactory
import mongo.MongoEventSinkFactory
import sql.mysql.{Engine, MySQLEventSinkFactory}
import collection.mutable.ArrayBuffer
import com.mongodb.WriteConcern
import sql.postgresql.PostgreSQLEventSinkFactory
import util.Random
import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 18:11
 */
object TestRunner {

  import utils._

  val deviceCount = 100
  val iters = 200000

  def main(args: Array[String]) {
    args(0) match {
      case "bdb" => runTest(new BerkeleyDBEventSinkFactory(new File("/other/berkeleydb-je")), iters, 1)
      //runTest(new LevelDBEventSinkFactory(new File("/other/leveldb-jni"), 1), iters, 1)
      case "mongo" => runTest(new MongoEventSinkFactory(100000, WriteConcern.NORMAL), iters, 1)
      case "myisam" => runTest(new MySQLEventSinkFactory(100000, Engine.MyISAM), iters, 1)
      case "innodb" => runTest(new MySQLEventSinkFactory(100000, Engine.InnoDB), iters, 1)
      case "psql" => runTest(new PostgreSQLEventSinkFactory(100000), iters, 1)
    }
  }

  def runTest(sinkFactory: EventSinkFactory, iters: Int, concurrencyLevel: Int) {
    def intSinks = (1 to 3).map("numbers" + _).map(sinkFactory.createEventSink[Int])
    def vecSinks = (1 to 3).map("vectors" + _).map(sinkFactory.createEventSink[(Double, Double)])
    def stringSinks = (1 to 3).map("strings" + _).map(sinkFactory.createEventSink[String])

    def sinks = ArrayBuffer() ++ intSinks ++ vecSinks ++ stringSinks
    sinks foreach {_.clear()}

    def configs = ArrayBuffer[EventGenerator.Config[_]]() ++
      intSinks.map {(_, (r: Random) => r.nextInt(20000), 5)} ++
      vecSinks.map {(_, (r: Random) => (r.nextDouble(), r.nextDouble()), 3)} ++
      stringSinks.map {(_, (r: Random) => 20 * r.nextPrintableChar().toString, 2)}

    val eg = Traversable.fill(concurrencyLevel)(new EventGenerator(deviceCount, iters, configs))
    val ((), time) = benchmark {eg.par.foreach(_.run())}

    val totalEvents = eg.map(_.totalEvents).sum
    println("%s took %s to insert %s events which is %s per sec.".format(sinkFactory.description, time, totalEvents, totalEvents / time))

    Thread.sleep(30000)
  }

}