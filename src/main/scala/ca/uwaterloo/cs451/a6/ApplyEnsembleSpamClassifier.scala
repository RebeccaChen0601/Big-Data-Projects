package ca.uwaterloo.cs451.a6

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.rogach.scallop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import scala.math.exp

class ApplyEnsembleSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val method = opt[String](descr = "method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new ApplyEnsembleSpamClassifierConf(argv)

        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Model: " + args.model())
        log.info("method: " + args.method())

		val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
        val sc = new SparkContext(conf)

        val outputDir = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

        val textFile = sc.textFile(args.input())
        val method = sc.textFile(args.method())

        val x_model = sc.textFile(args.model() + "/part-00000")
        val x_weight = x_model.map(line => {
                            val tokens = line.split("\\(|,|\\)")
                            (tokens(1).toInt, tokens(2).toDouble)
                        }).collectAsMap()
        val x_w = sc.broadcast(x_weight)

        val y_model = sc.textFile(args.model() + "/part-00001")
        val y_weight = y_model.map(line => {
                            val tokens = line.split("\\(|,|\\)")
                            (tokens(1).toInt, tokens(2).toDouble)
                        }).collectAsMap()
        val y_w = sc.broadcast(y_weight)

        val bri_model = sc.textFile(args.model() + "/part-00002")
        val bri_weight = bri_model.map(line => {
                            val tokens = line.split("\\(|,|\\)")
                            (tokens(1).toInt, tokens(2).toDouble)
                        }).collectAsMap()
        val bri_w = sc.broadcast(bri_weight)

        // Scores a document based on its list of features.
        def spamminess(features: Array[Int], w: scala.collection.Map[Int, Double]) : Double = {
            var score = 0d
            features.foreach(f => if (w.contains(f)) score += w(f))
            score
        }

         val tested = textFile.map(line =>{
                // Parse input
                // For each instance...     
                val tokens = line.split(" ")
                val docid = tokens(0)
                val label = tokens(1)
                val features = tokens.drop(2).map(tokens => tokens.toInt) // feature vector of the training instance
                var score = 0d
                val x_score = spamminess(features, x_w.value)
                val y_score = spamminess(features, y_w.value)
                val bri_score = spamminess(features, bri_w.value)
                if (method == "average") {  
                    score = (x_score + y_score + bri_score) / 3
                } else {
                    val x_vote = if (x_score > 0) 1 else -1
                    val y_vote = if (y_score > 0) 1 else -1
                    val bri_vote = if (bri_score > 0) 1 else -1
                    score = x_vote + y_vote + bri_vote
                }
                val prediction = if (score > 0) "spam" else "ham"  
                
                (docid, label, score,  prediction)
            })
        tested.saveAsTextFile(args.output())
    }
}