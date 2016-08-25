:load scripts/utils.scala
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._

val covtype = sc.textFile("data/covtype/covtype.data")
val data = covtype.map(_.split(",").toDouble).map(values => LabeledPoint(values.last-1, Vectors.dense(values.init)))

val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8,0.1,0.1))
trainData.cache
cvData.cache
testData.cache()

