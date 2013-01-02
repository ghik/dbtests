package com.ghik

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 29.12.12
 * Time: 17:19
 * To change this template use File | Settings | File Templates.
 */
case class Event[T](deviceId: Int, timestamp: Long, data: T) {
}
