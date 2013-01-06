package com.ghik.mysql

/**
 * Created with IntelliJ IDEA.
 * User: ghik
 * Date: 06.01.13
 * Time: 15:41
 */
sealed abstract class Engine

object Engine {

  case object MyISAM extends Engine {
    override def toString = "myisam"
  }

  case object InnoDB extends Engine {
    override def toString = "innodb"
  }

}
