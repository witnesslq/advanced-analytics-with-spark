:load scripts/NAStatCounter.scala
import org.apache.spark.rdd.RDD

def some[T](exp: => T) =
	try { Some(exp) } catch { case e:Exception => None }

object StatUtils extends Serializable {
    implicit class Stat(rdd: RDD[Array[Double]]) extends Serializable {
	        def arrstats = (rdd.map(_.map(NAStatCounter(_)))
	                           .mapPartitions(iter => Iterator(iter.reduce(_.zip(_).map{case (a,b) => a.merge(b)})))
                               .reduce(_.zip(_).map{case (a,b) => a.merge(b)}))
    }
}

object ArrayUtils extends Serializable {
    implicit class Utils(arr: Array[String]) extends Serializable {
		def toInt = arr.map(_.toInt)
        def toDouble = arr.map(_.toDouble)
    }
}

import StatUtils._
import ArrayUtils._

