:load scripts/NAStatCounter.scala

case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

def parse(line: String): MatchData = {
	val values = line.split(",")
	MatchData(id1 = values(0).toInt, 
			  id2 = values(1).toInt,
			  scores = values.slice(2,11).map(x => if (x == "?") Double.NaN else x.toDouble), 
			  matched = values(11).toBoolean)
}

val text = sc.textFile("data/linkage/")

val parsed = text.filter(!_.startsWith("\"id_1\"")).map(parse)

val stats = parsed.map(_.scores).map(_.map(NAStatCounter(_))).mapPartitions(iter => Iterator(iter.reduce(_.zip(_).map{case (a,b) => a.merge(b)}))).reduce(_.zip(_).map{case (a,b) => a.merge(b)})

stats.foreach(println)
