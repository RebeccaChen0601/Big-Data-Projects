package ca.uwaterloo.cs451.a6

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.rogach.scallop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import scala.math.exp

class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  verify()
}

object ApplySpamClassifier {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new ApplySpamClassifierConf(argv)

        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
		log.info("Model: " + args.model())

		val conf = new SparkConf().setAppName("ApplySpamClassifier")
        val sc = new SparkContext(conf)

        val outputDir = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

        val textFile = sc.textFile(args.input())
        val model = sc.textFile(args.model() + "/part-00000")
     
        val weight = model.map(line => {
                            val tokens = line.split("\\(|,|\\)")
                            (tokens(1).toInt, tokens(2).toDouble)
                        }).collectAsMap()
        val w = sc.broadcast(weight)

        // Scores a document based on its list of features.
        def spamminess(features: Array[Int]) : Double = {
            var score = 0d
            features.foreach(f => if (w.value.contains(f)) score += w.value(f))
            score
        }

        val tested = textFile.map(line =>{
                // Parse input
                // For each instance...     
                val tokens = line.split(" ")
                val docid = tokens(0)
                val label = tokens(1)
                val features = tokens.drop(2).map(tokens => tokens.toInt) // feature vector of the training instance
                val score = spamminess(features)
                val prediction = if (score > 0) "spam" else "ham"  
                
                (docid, label, score,  prediction)
            })
        tested.saveAsTextFile(args.output())
    }
}