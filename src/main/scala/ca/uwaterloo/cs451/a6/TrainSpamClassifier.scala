package ca.uwaterloo.cs451.a6

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.rogach.scallop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import scala.math.exp

class TrainSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val shuffle = opt[Boolean](descr = "shuffle the data", required = false)
  verify()
}

object TrainSpamClassifier {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new TrainSpamClassifierConf(argv)

		log.info("Input: " + args.input())
		log.info("Model: " + args.model())

		val conf = new SparkConf().setAppName("TrainSpamClassifier")
        val sc = new SparkContext(conf)

        var textFile = sc.textFile(args.input())

        val outputDir = new Path(args.model())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

        // w is the weight vector (make sure the variable is within scope)
        val w = scala.collection.mutable.Map[Int, Double]()

        // Scores a document based on its list of features.
        def spamminess(features: Array[Int]) : Double = {
            var score = 0d
            features.foreach(f => if (w.contains(f)) score += w(f))
            score
        }

        // This is the main learner:
        val delta = 0.002


        // if shuffle is required
        if (args.shuffle()) {
            textFile = textFile.map(line => (scala.util.Random.nextInt, line))
                               .sortByKey()
                               .map(line => line._2)

        }

        val trained = textFile.map(line =>{
                // Parse input

                // For each instance...
                val tokens = line.split(" ")
                val isSpam =  if (tokens(1) == "spam") 1 else 0 // label
                val features = tokens.drop(2).map(tokens => tokens.toInt) // feature vector of the training instance
                val docid = tokens(0)
                
                (0, (docid, isSpam, features))
            }).groupByKey(1)
            .flatMap(line => {
                // Then run the trainer...
                // Update the weights as follows:
                line._2.foreach(instance => {
                    val features = instance._3
                    val isSpam = instance._2
                    
                    val score = spamminess(features)
                    val prob = 1.0 / (1 + exp(-score))
                    features.foreach(f => {
                        if (w.contains(f)) {
                            w(f) += (isSpam - prob) * delta
                        } else {
                            w(f) = (isSpam - prob) * delta
                        }
                    })
                })
                w
            })
        trained.saveAsTextFile(args.model())   
    }
}