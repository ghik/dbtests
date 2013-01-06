package com.ghik.mongo

import com.ghik.utils._
import com.mongodb.{DB, BasicDBObject, DBObject, Mongo}
import collection.mutable.ArrayBuffer
import util.Random
import collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 06.01.13
 * Time: 22:42
 */
object NativeMongoTest {
  def main(args: Array[String]) {
    runTest(10000, 1000000)
  }

  def runTest(batchSize: Int, count: Int) {
    val db: DB = (new Mongo).getDB("stuff")
    db.dropDatabase()

    val coll = db.getCollection("stuff")
    coll.ensureIndex(new BasicDBObject("deviceId", 1))
    coll.ensureIndex(new BasicDBObject("timestamp", 1).append("deviceId", 1))

    var batch = new ArrayBuffer[DBObject](batchSize)
    val r = new Random

    def flush() {
      coll.insert(batch: java.util.List[DBObject])
      batch = new ArrayBuffer[DBObject]
    }

    val ((), time) = benchmark {
      count times {
        val obj = new BasicDBObject
        obj.put("deviceId", r.nextInt(100))
        obj.put("timestamp", System.currentTimeMillis)
        obj.put("data", r.nextInt(20000))

        batch += obj
        if (batch.size >= batchSize) {
          flush()
        }
      }

      flush()
    }

    printf("It took %f to insert %d which is %f per second.\n", time, count, count / time)
  }
}
