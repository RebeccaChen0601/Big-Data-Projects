package ca.uwaterloo.cs451.a5

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.rogach.scallop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


class Q4Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "the plain-text version of the data", required = false)
  val parquet = opt[Boolean](descr = "the parquet version of the data", required = false)
  verify()
}

object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q4Conf(argv)

    log.info("Input: " + args.input())
    log.info("Ship date: " + args.date())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)
    val date = args.date()
    val input = args.input()

    if (args.parquet()){
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(input + "/lineitem")
        val nationDF = sparkSession.read.parquet(input + "/nation")
        val customerDF = sparkSession.read.parquet(input + "/customer") 
        val orderDF = sparkSession.read.parquet(input + "/orders")
        val lineitemRDD = lineitemDF.rdd
        val orderRDD = orderDF.rdd
        val nationRDD = nationDF.rdd
        val customerRDD = customerDF.rdd
        val nationTable = nationRDD
            .map(nation => (nation.getInt(0), nation.getString(1)))
        val orderTable = orderRDD
            .map(order => (order.getInt(0), order.getInt(1)))
        val customerTable = customerRDD
            .map(customer => (customer.getInt(0), customer.getInt(3)))
        val nationMap = sc.broadcast(nationTable.collectAsMap())
        val customerMap = sc.broadcast(customerTable.collectAsMap())
        lineitemRDD
            .filter(line => line.getString(10).contains(date))
            .map(line => (line.getInt(0), 1))
            .reduceByKey(_ + _)
            .cogroup(orderTable)
            .filter(_._2._1.size != 0)
            .map(line => {
                val nationKey = customerMap.value(line._2._2.head)
                val nationName = nationMap.value(nationKey)
                ((nationKey, nationName), line._2._1.iterator.next())
            })
            .reduceByKey(_ + _)
            .map(line => (line._1._1, (line._1._2, line._2)))
            .sortBy(_._1)
            .collect()
            .foreach(tuple => println(tuple._1, tuple._2._1, tuple._2._2))
        
    } else if (args.text()) {
        val text = sc.textFile(input + "/lineitem.tbl")
        val nation = sc.textFile(input + "/nation.tbl")
        val orders = sc.textFile(args.input() + "/orders.tbl")
        val customers = sc.textFile(input + "/customer.tbl")
        val nationTable = nation.map(nation => (nation.split("\\|")(0).toInt, nation.split("\\|")(1).toString))
        val orderTable = orders.map(order => (order.split("\\|")(0).toInt, order.split("\\|")(1).toInt))
        val customerTable = customers.map(customer => (customer.split("\\|")(0).toInt, customer.split("\\|")(3).toInt))
        val nationMap = sc.broadcast(nationTable.collectAsMap())
        val customerMap = sc.broadcast(customerTable.collectAsMap())
        text
            .filter(line => line.split("\\|")(10).contains(date))
            .map(line => (line.split("\\|")(0).toInt, 1))
            .reduceByKey(_ + _)
            .cogroup(orderTable)
            .filter(_._2._1.size != 0)
            .map({line => 
                val nationKey = customerMap.value(line._2._2.head)
                val nationName = nationMap.value(nationKey)
                ((nationKey, nationName), line._2._1.iterator.next())
            })
            .reduceByKey(_ + _)
            .map(line => (line._1._1, (line._1._2, line._2)))
            .sortBy(_._1)
            .collect()
            .foreach(tuple => println(tuple._1, tuple._2._1, tuple._2._2))
    }
    
  }
}
