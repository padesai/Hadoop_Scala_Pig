package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

case class Edge(source: String, target: String, weight: String)

object Q2 {

	def main(args: Array[String]) {
    	val sc = new SparkContext(new SparkConf().setAppName("Q2"))
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._

    	// read the file
    	val file = sc.textFile("hdfs://localhost:8020" + args(0))
		/* TODO: Needs to be implemented */
	val edges  = file.map(_.split("\t")).map(e => Edge(e(0), e(1), e(2))).toDF()

	val weightsLessThanTen = edges.filter(edges("weight") >= 10)

	val averageOutgoingWeights = weightsLessThanTen.groupBy("source").agg(avg("weight").as("weight1")).select("source", "weight1") 
	val averageIncomingWeights = weightsLessThanTen.groupBy("target").agg(avg("weight").as("weight2")).select("target", "weight2")
	
	val output = averageOutgoingWeights.join(averageIncomingWeights, $"source" === $"target").select($"source", $"weight1" - $"weight2")

    	// store output on given HDFS path.
    	// YOU NEED TO CHANGE THIS
    	output.map(x => x(0) + "\t" + x(1)).saveAsTextFile("hdfs://localhost:8020" + args(1))
  	
	}
}
