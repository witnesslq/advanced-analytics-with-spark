val rawArtistData = sc.textFile("data/profiledata_06-May-2005/artist_data.txt")
val rawUserArtistData = sc.textFile("data/profiledata_06-May-2005/user_artist_data.txt")
val rawArtistAlias = sc.textFile("data/profiledata_06-May-2005/artist_alias.txt")

val artistAlias = (rawArtistAlias.map(_.split("\t"))
								 .filter(_.length == 2)
								 .flatMap(ids => try { Some((ids(0).toInt,ids(1).toInt)) } 
									 			 catch { case e:Exception => None }).collectAsMap)
val bartistAlias = sc.broadcast(artistAlias)

