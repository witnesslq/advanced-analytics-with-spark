import org.apache.spark.mllib.recommendation._
:load scripts/utils.scala

val rawArtistData = sc.textFile("data/profiledata_06-May-2005/artist_data.txt")
val rawUserArtistData = sc.textFile("data/profiledata_06-May-2005/user_artist_data_part.txt")
val rawArtistAlias = sc.textFile("data/profiledata_06-May-2005/artist_alias.txt")

val artistAlias = (rawArtistAlias.map(_.split("\t").map(_.trim))
								 .filter(_.length == 2)
								 .flatMap{case Array(id1,id2) => some(id1.toInt,id2.toInt)}
								 .collectAsMap)

val bartistAlias = sc.broadcast(artistAlias)

val artistById = (rawArtistData.map(_.split("\t", 2).map(_.trim))
							   .filter(_.length == 2)
							   .flatMap{case Array(id,name) => some(id.toInt, name)})
// model training
val trainData = (rawUserArtistData.map(_.split(" ").map(_.trim.toInt))
								  .map{case Array(user,artist,count) => 
									   Rating(bartistAlias.value.getOrElse(user,user),artist,count)})

val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

model.userFeatures.mapValues(_.mkString(", ")).first

val user = 1000002
val num = 5
println("user " + user + " listend: ")
val listend = trainData.filter(_.user==user).map(_.product).collect.toSet
artistById.filter(artist => listend.contains(artist._1)).map(_._2).collect.toSet.foreach(println)

val recommends = model.recommendProducts(user, num)
println("the recommends is:")
recommends.foreach(println)
val recArtist = recommends.map(_.product).toSet

artistById.filter(artist => recArtist.contains(artist._1)).map(_._2).collect.toSet.foreach(println)

