package ca.uwaterloo.cs451.a5

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.rogach.scallop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


class Q1Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "the plain-text version of the data", required = false)
  val parquet = opt[Boolean](descr = "the parquet version of the data", required = false)
  verify()
}

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q1Conf(argv)

    log.info("Input: " + args.input())
    log.info("Ship date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)
    val date = args.date()
    val input = args.input()
    if (args.parquet()){
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(input + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        val count = lineitemRDD
            .map(line => line.getString(10))
            .filter(line => line.contains(date))
            .count()
        println("ANSWER=" + count)
    } else if (args.text()) {
        val text = sc.textFile(args.input() + "/lineitem.tbl")
        val count = text
            .map(line => line.split("\\|")(10))
            .filter(line => line.contains(date))
            .count()
        println("ANSWER=" + count)
    }
    
  }
}