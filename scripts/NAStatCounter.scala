import org.apache.spark.util.StatCounter
import java.lang.Double.isNaN

class NAStatCounter extends Serializable {
	val stat = new StatCounter
	var missing = 0

	def add(x: Double): NAStatCounter = {
		if (isNaN(x))
			missing += 1
		else
			stat.merge(x)
		this
	}

	def merge(other: NAStatCounter): NAStatCounter = {
		stat.merge(other.stat)
		missing += other.missing
		this
	}

	override def toString = 
			"missing: " + missing + "; stat: " + stat
}

object NAStatCounter extends Serializable {
	def apply(x: Double): NAStatCounter = {
		new NAStatCounter().add(x)
	}
}

