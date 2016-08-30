:load scripts/utils.scala

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

// val stats = parsed.map(_.scores).arrstats
// stats.foreach(println)

val statsm = parsed.filter(_.matched).map(_.scores).arrstats
val statsn = parsed.filter(!_.matched).map(_.scores).arrstats

println("-- matched stats --------------------")
statsm.foreach(println)
println("-- not matched stats -----------------")
statsn.foreach(println)
println("-- differences -----------------------")
statsm.zip(statsn).map{case (x,y) => (x.missing + y.missing, x.stat.mean - y.stat.mean)}.foreach(println)

def nan(d: Double) = if (d == Double.NaN) 0 else d
val ct = parsed.map(data => (Array(2,5,6,7,8).map(data.scores(_)).map(nan).sum, data))

ct.filter(_._1 > 4.0).map(_._2.matched).countByValue
// ~: true -> 19868, false -> 376
ct.filter(_._1 > 3.0).map(_._2.matched).countByValue
// ~: res12: scala.collection.Map[Boolean,Long] = Map(true -> 20862, false -> 223430)

