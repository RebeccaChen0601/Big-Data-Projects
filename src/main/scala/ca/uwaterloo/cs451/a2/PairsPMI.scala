package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math.log10

class ConfPairsPMI(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPairsPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
    var first_sum = 0.0f
    var second_sum = 0.0f
    val threshold = sc.broadcast(args.threshold())
    val reducers = args.reducers()
    val textFile = sc.textFile(args.input())
    val line_count = sc.broadcast(textFile.count())
    val pairsPMI = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(40).distinct
        if (tokens.length > 1){
          // map all the non-repeated pairs and each single word 
            tokens.flatMap(firstToken => tokens.map(secondToken => (firstToken, secondToken)) ++ List((firstToken, "*")))
                  .filter(pair => pair._1 != pair._2)
        } else List()
      })
      .map(pair => (pair, 1)) // Add base frequency 1 for each word
      .reduceByKey(_ + _, reducers) // Found total count of each word
      .sortByKey()
      .map(pair => 
        if (pair._1._2 == "*") {
          // count the number of appearance for the first word
            first_sum = pair._2
            ((pair._1._1, pair._1._2), (0.0f, pair._2))
        } else {
          // switch the order of the first and second word
            ((pair._1._2, pair._1._1), (pair._2 / first_sum, pair._2))
        })
      .filter(pair => (pair._2._2 >= threshold.value))
      .sortByKey()
      .map(pair =>
        if (pair._1._2 == "*") {
          // count the number of appearance for the second word
            second_sum = pair._2._2
            pair
        } else {
            ((pair._1._1, pair._1._2), (log10((line_count.value * pair._2._1 / second_sum).toDouble), pair._2._2))
        })
      .filter(pair => (pair._1._2 != "*"))
    pairsPMI.saveAsTextFile(args.output())
  }
}