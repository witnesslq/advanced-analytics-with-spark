:load scripts/utils.scala
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._

val rawData = sc.textFile("data/covtype/covtype.data")

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

(0 until 7).map(cat => (cat, metrics.precision(cat), metrics.recall(cat))).foreach(println)

// compute probabilities for each category
def classProbabilities(data: RDD[LabeledPoint]): Map[Double,Double] = {
	val countsByCategory = data.map(_.label).countByValue
	val sum = countsByCategory.map(_._2).sum
	countsByCategory.map(cate => (cate._1, cate._2.toDouble / sum)).toMap
}

val trainProb = classProbabilities(trainData)
val cvProb = classProbabilities(cvData)
// random probabilities
val totalProb = trainProb.map{ case(cat,prob) => cvProb.getOrElse(cat,0d) * prob }.sum
println("total prob is " + totalProb) // ~: about 0.3760411721535429

// BETTER PARAMETERS
val model = DecisionTree.trainClassifier(trainData, 7, Map[Int,Int](), "entropy", 20, 300)

val metrics = getMetrics(model, cvData)
metrics.confusionMatrix
println("precision is: " + metrics.precision)

// TEST
val model = DecisionTree.trainClassifier(trainData.union(cvData), 7, Map[Int,Int](), "entropy", 20, 300)
val metrics = getMetrics(model, testData)
metrics.confusionMatrix
println("precision is: " + metrics.precision)

// trait feature 10,11 as category feature, do not use 1-hot encode
val data = (rawData.map(_.split(",").toDouble)
				   .map(values => values.slice(0,10) :+ values.slice(10,14).indexOf(1d).toDouble 
					   			  :+ values.slice(14,54).indexOf(1d).toDouble :+ values.last)
				   .map(values => LabeledPoint(values.last-1, Vectors.dense(values.init))))
val Array(trainData,cvData,testData) =
				data.randomSplit(Array(0.8,0.1,0.1))
trainData.cache
cvData.cache
testData.cache

val model = DecisionTree.trainClassifier(trainData, 7, Map[Int,Int](10 -> 4, 11 -> 40), "entropy", 30, 300)
val train_metrics = getMetrics(model, trainData)
val cv_metrics = getMetrics(model, cvData)

println("train accuracy is " + train_metrics.precision)
println("cv accuracy is " + cv_metrics.precision)

val model = DecisionTree.trainClassifier(trainData.union(cvData), 7, Map[Int,Int](10 -> 4, 11 -> 40), "entropy", 30, 300)
val test_metrics = getMetrics(model, testData)
println("test accuracy is " + test_metrics.precision)

// RANDOM FOREST
val forest = RandomForest.trainClassifier(trainData, 7, Map(10 -> 4, 11 -> 40), 20, "auto", "entropy", 20, 300)

val predictionAndLabels = trainData.map(example =>(forest.predict(example.features), example.label))
val train_metrics = new MulticlassMetrics(predictionAndLabels)
val predictionAndLabels = cvData.map(example =>(forest.predict(example.features), example.label))
val cv_metrics = new MulticlassMetrics(predictionAndLabels)

println("train accuracy is " + train_metrics.precision)
println("cv accuracy is " + cv_metrics.precision)

// PREDICT
val input = "2709,125,28,67,23,3324,253,207,61,6094,0,29"
val vector = Vectors.dense(input.split(",").toDouble)
forest.predict(vector) // ~: 4.0

