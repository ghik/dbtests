package com.ghik

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 05.01.13
 * Time: 21:43
 */
package object utils {
  implicit class RichInt(i: Int) {
    def times(fun: => Any) {
      var c = 0
      while (c < i) {
        fun
        c += 1
      }
    }

    def *(s: String) = s * i
  }
}
