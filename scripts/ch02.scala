:load scripts/NAStatCounter.scala

case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

def parse(line: String): MatchData = {
	val values = line.split(",")
	MatchData(values(0).toInt, values(1).toInt,
			values.slice(2,11).map(x => if(x == "?") Double.NaN else x.toDouble), 
			values(11).toBoolean)
}

text.filter(!_.startsWith("\"id_1\"")).map(parse).take(1000).map(_.scores.map(NAStatCounter.apply)).reduce(_.zip(_).map{case (a,b) => a.merge(b)}).foreach(println)
