package ca.uwaterloo.cs451.a5

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.rogach.scallop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "the plain-text version of the data", required = false)
  val parquet = opt[Boolean](descr = "the parquet version of the data", required = false)
  verify()
}

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q2Conf(argv)

    log.info("Input: " + args.input())
    log.info("Ship date: " + args.date())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)
    val date = args.date()
    val input = args.input()

    if (args.parquet()){
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(input + "/lineitem")
        val orderDF = sparkSession.read.parquet(input + "/orders")
        val lineitemRDD = lineitemDF.rdd
        val orderRDD = orderDF.rdd
        val orderKeys = orderRDD
            .map(order => (order.getInt(0), order.getString(6)))
        lineitemRDD
            .map(line => (line.getInt(0), line.getString(10)))
            .filter(line => line._2.contains(date))
            .cogroup(orderKeys)
            .filter(_._2._1.size != 0)
            .sortBy(_._1)
            .map(line => (line._2._2.head, line._1.toString.toLong))
            .take(20)
            .foreach(tuple => println(tuple))
        
    } else if (args.text()) {
        val text = sc.textFile(input + "/lineitem.tbl")
        val orders = sc.textFile(input + "/orders.tbl")
        val orderKeys = orders.map(order => (order.split("\\|")(0).toInt, order.split("\\|")(6).toString))
        text
            .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(10).toString))
            .filter(line => line._2.contains(date))
            .cogroup(orderKeys)
            .filter(_._2._1.size != 0)
            .sortBy(_._1)
            .map(line => (line._2._2.head, line._1.toLong))
            .take(20)
            .foreach(tuple => println(tuple))
    }
    
  }
}