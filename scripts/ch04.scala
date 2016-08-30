:load scripts/utils.scala
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._

val rawData = sc.textFile("data/covtype.data")

val data = (rawData.map(_.split(",").toDouble)
				   .map(values => LabeledPoint(values.last-1,Vectors.dense(values.init))))

val Array(trainData,cvData,testData) = 
		data.randomSplit(Array(0.8,0.1,0.1))

trainData.cache
cvData.cache
testData.cache

import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._

def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]) = {
	val predictionAndLabels = data.map(example => 
		(model.predict(example.features), example.label))
	new MulticlassMetrics(predictionAndLabels)
}

val model = DecisionTree.trainClassifier(trainData, 7, Map[Int,Int](), "gini", 4, 100)

val metrics = getMetrics(model, cvData)

metrics.confusionMatrix
println("precision is: " + metrics.precision)

