package com.ghik

import com.sleepycat.je.{DatabaseConfig, Environment, EnvironmentConfig}
import java.io.File
import org.iq80.leveldb.Options
import org.fusesource.leveldbjni.JniDBFactory

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 06.01.13
 * Time: 15:02
 */
object Playground {
  def main(args: Array[String]) {
    testLevelDb()
  }

  def testBerkeleyDb() {
    val envConf = new EnvironmentConfig
    envConf.setAllowCreate(true)

    val dbEnv = new Environment(new File("/home/ghik/bdb"), envConf)

    val dbConf = new DatabaseConfig
    dbConf.setAllowCreate(true)

    val db = dbEnv.openDatabase(null, "testorz", dbConf)
  }

  def testLevelDb() {
    import JniDBFactory._

    val options = new Options().createIfMissing(true)

    val db = factory.open(new File("example"), options)
    try {
      db.put(bytes("lolzsy"), bytes("dafuqz"))

      println(asString(db.get(bytes("lolzsy"))))
    } finally {
      // Make sure you close the db to shutdown the
      // database and avoid resource leaks.
      db.close()
    }
  }
}
