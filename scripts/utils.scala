def some[T](exp: => T) = 
    try {Some(exp)} catch {case e:Exception => None}

object StringUtils extends Serializable {
    implicit class util(s: String) extends Serializable {
    	def trimInt  = Integer.parseInt(s.trim)
	}   
}

object ArrayUtils extends Serializable {
    implicit class Util(arr: Array[String]) extends Serializable {
        def toInt  = arr.map(_.trim.toInt)
		def toDouble = arr.map(_.trim.toDouble)
    }
}


import StringUtils._
import ArrayUtils._

