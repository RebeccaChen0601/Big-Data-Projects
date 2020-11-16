package ca.uwaterloo.cs451.a5

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.rogach.scallop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


class Q6Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "the plain-text version of the data", required = false)
  val parquet = opt[Boolean](descr = "the parquet version of the data", required = false)
  verify()
}

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q6Conf(argv)

    log.info("Input: " + args.input())
    log.info("Ship date: " + args.date())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)
    val date = args.date()
    val input = args.input()
    if (args.parquet()){
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(input + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        val count = lineitemRDD
            .filter(line => line.getString(10).contains(date))
            .map(line => {
                val quantity = line.getDouble(4)
                val basePrice = line.getDouble(5)
                val discount = line.getDouble(6)
                val tax = line.getDouble(7)
                val discPrice = basePrice * (1 - discount)
                val charge = discPrice * (1 + tax)
                ((line.getString(8), line.getString(9)),
                (quantity, basePrice, discPrice, charge, discount, 1))
            })
            .reduceByKey((x, y) => (x._1+y._1, x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5, x._6+y._6))
            .collect()
            .foreach(p => println(p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, p._2._1/p._2._6, p._2._2/p._2._6, p._2._5/p._2._6, p._2._6))
    } else if (args.text()) {
        val text = sc.textFile(args.input() + "/lineitem.tbl")
        val count = text
            .filter(line => line.split("\\|")(10).contains(date))
            .map(line => {
                val quantity = line.split("\\|")(4).toDouble
                val basePrice = line.split("\\|")(5).toDouble
                val discount = line.split("\\|")(6).toDouble
                val tax = line.split("\\|")(7).toDouble
                val discPrice = basePrice * (1 - discount)
                val charge = discPrice * (1 + tax)
                ((line.split("\\|")(8), line.split("\\|")(9)),
                (quantity, basePrice, discPrice, charge, discount, 1))
            })
            .reduceByKey((x, y) => (x._1+y._1, x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5, x._6+y._6))
            .collect()
            .foreach(p => println(p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, p._2._1/p._2._6, p._2._2/p._2._6, p._2._5/p._2._6, p._2._6))
    }
    
  }
}