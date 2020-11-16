package ca.uwaterloo.cs451.a5

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.rogach.scallop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


class Q7Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "the plain-text version of the data", required = false)
  val parquet = opt[Boolean](descr = "the parquet version of the data", required = false)
  verify()
}

object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q7Conf(argv)

    log.info("Input: " + args.input())
    log.info("Ship date: " + args.date())

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)
    val date = args.date()
    val input = args.input()

    if (args.parquet()){
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(input + "/lineitem")
        val customerDF = sparkSession.read.parquet(input + "/customer") 
        val orderDF = sparkSession.read.parquet(input + "/orders")
        val lineitemRDD = lineitemDF.rdd
        val customerRDD = customerDF.rdd
        val orderRDD = orderDF.rdd
        val customerTable = customerRDD
            .map(customer => (customer.getInt(0), customer.getString(1)))
        val customerMap = sc.broadcast(customerTable.collectAsMap())
        val orderTable = orderRDD
            .filter(order => (order.getString(4) < date))
            .filter(order => customerMap.value.contains(order.getInt(1)))
            .map(order => (order.getInt(0), (order.getInt(1),
                         order.getString(4), order.getInt(7))))
        lineitemRDD
            .filter(line => line.getString(10) > date)
            .map(line => (line.getInt(0), (line.getDouble(5) * (1 - line.getDouble(6)))))
            .reduceByKey(_ + _)
            .cogroup(orderTable)
            .filter(line => line._2._1.size != 0 && line._2._2.size != 0)
            .map(line => {
                ((customerMap.value(line._2._2.head._1), line._1, line._2._2.head._2, line._2._2.head._3),
                line._2._1.head)
            })
            .sortBy(_._2)
            .take(10)
            .foreach(tuple => println(tuple._1._1, tuple._1._2, tuple._2, tuple._1._3, tuple._1._4))
        
    } else if (args.text()) {
        val text = sc.textFile(input + "/lineitem.tbl")
        val orders = sc.textFile(args.input() + "/orders.tbl")
        val customers = sc.textFile(input + "/customer.tbl")
        val customerTable = customers.map(customer => (customer.split("\\|")(0).toInt, customer.split("\\|")(1).toString))
        val customerMap = sc.broadcast(customerTable.collectAsMap())
        val orderTable = orders.filter(order => (order.split("\\|")(4) < date))
                               .filter(order => customerMap.value.contains(order.split("\\|")(1).toInt))
                               .map(order => (order.split("\\|")(0).toInt, (order.split("\\|")(1).toInt,
                                            order.split("\\|")(4), order.split("\\|")(7).toInt)))
        text
            .filter(line => line.split("\\|")(10) > date)
            .map(line => (line.split("\\|")(0).toInt, (line.split("\\|")(5).toDouble * (1 - line.split("\\|")(6).toDouble))))
            .reduceByKey(_ + _)
            .cogroup(orderTable)
            .filter(line => line._2._1.size != 0 && line._2._2.size != 0)
            .map(line => {
                ((customerMap.value(line._2._2.head._1), line._1, line._2._2.head._2, line._2._2.head._3),
                line._2._1.head)
            })
            .sortBy(_._2)
            .take(10)
            .foreach(tuple => println(tuple._1._1, tuple._1._2, tuple._2, tuple._1._3, tuple._1._4))
    }
    
  }
}