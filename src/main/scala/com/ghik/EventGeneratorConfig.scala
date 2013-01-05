package com.ghik

import util.Random

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 05.01.13
 * Time: 16:10
 */

case class EventGeneratorConfig[T](sink: EventSink[T], dataGen: Random => T, count: Int)