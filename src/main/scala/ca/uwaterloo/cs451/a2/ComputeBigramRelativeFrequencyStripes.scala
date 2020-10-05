package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ConfStripe(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfStripe(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    var sum = 0.0f
    val reducers = args.reducers()
    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          tokens.sliding(2).map(words => {
            (words(0), Map(words(1) -> 1.0f))
          })
        } else {
            List()
        }
      }) 
      .reduceByKey((x, y) => {
        var word_to_map = x 
        for ((k, v) <- y) {
          word_to_map += (k-> (word_to_map.getOrElse(k, 0.0f) + v))
        }
        word_to_map
      }, reducers)
      .map(bigram => {
        var count = 0.0f
        val first_word = bigram._1
        val second_map = bigram._2
        for((inner_key, inner_value) <- second_map) {
          count += inner_value
        }
        var inner_map = Map[String, Float]()
        for((inner_key, inner_value) <- second_map) {
          inner_map += (inner_key -> (inner_value/count))
        }
        (first_word, inner_map)
      })  
    counts.saveAsTextFile(args.output())
  }
}



          // In this map we generate {word: {n1: freq, n2: freq, ...}} structure
          // based on the word list

          // var word_to_ns = List[(String, Map[String, Int])]()
          // var word_to_map = Map[(String, Map[String, Int])]()
          
            
            
          
            // words.sliding(2).foreach((a, b) => {
            //   if (word_to_map.contains(a)) {
            //     // Check if the first word exists in the outer map
            //     var map = word_to_map(a)
            //     if (map.contains(b)) {
            //       // Check if the second word exists in the inner map
            //       map += (b -> map.get(b) + 1)
            //     } else {
            //       map += (b -> 1)
            //     }
            //     // word_to_ns = word_to_ns :+ (a -> map)  
            //   } else {
            //     var map = Map[String, Int]()
            //     map += (b -> 1)
            //     word_to_map += (a -> map)
            //     // word_to_ns = word_to_ns :+ (a -> map)
            //   }
            // })
            // word_to_map
         