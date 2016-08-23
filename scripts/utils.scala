def some[T](exp: => T) = {
    try {Some(exp)} catch {case e:Exception => None}
}

object StringUtils extends Serializable {
    implicit class util(s: String) extends Serializable {
    	def toInteger  = Integer.parseInt(s.trim)
	}   
}

import StringUtils._
