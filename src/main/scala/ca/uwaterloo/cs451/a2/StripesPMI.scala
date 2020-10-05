package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math.log10

class StripePMIConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "max number of words to check in each line", required = true)
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new StripePMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("StripesPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val line_count = sc.broadcast(scala.io.Source.fromFile(args.input()).getLines.size)
    val reducers = args.reducers()

    // Find the line count of each individual word as well as the total line
    // number
    val wordToCount = textFile
      .flatMap(line => {
        tokenize(line).take(40).distinct
      })
      .map((_, 1)) // Add base frequency 1 for each word
      .reduceByKey(_+_, reducers) // Found total count of each word
      .collectAsMap()

    val broadcastVar = sc.broadcast(wordToCount)
    val threshold = sc.broadcast(args.threshold())

    val stripespmi = textFile
      .map(line => {
        tokenize(line).take(40).distinct
      })
      .flatMap(words => {
        // In this map we generate {word: {n1: freq, n2: freq, ...}} structure
        // based on the word list
        var word_to_ns = List[(String, Map[String, Int])]()
        if (words.length < 2) {
          word_to_ns
        } else {
          words.foreach(a => {
            var map = Map[String, Int]()
            words.foreach(b => {
              if (a != b) {
                map += (b -> 1)
              }
            })
            word_to_ns = word_to_ns :+ (a, map)
          })
          word_to_ns
        }
      })
      .reduceByKey((a, b) => {
        // In this map we accumulate the pair frequency
        var word_to_ns = a

        for ((k, v) <- b) {
          word_to_ns += (k -> (word_to_ns.getOrElse(k, 0) + v))
        }
        // Convert to map to reduce data size for sending out and keep the
        // consistency of the output from the previous map function
        word_to_ns
      }, reducers).map(key => {
      val word = key._1
      val n_to_freq = key._2
      val thres = threshold.value

      var wordToRatioMap = Map[String, (Double, Int)]()
      for ((k, v) <- n_to_freq) {
        if (v >= thres) {
          wordToRatioMap += (k -> (log10(v * line_count.value / (broadcastVar.value(k) * broadcastVar.value(word)).toDouble), v))
        }
      }
      if (!wordToRatioMap.isEmpty ) {
        (word, wordToRatioMap)
      }
    })
    stripespmi.saveAsTextFile(args.output())
  }
}
