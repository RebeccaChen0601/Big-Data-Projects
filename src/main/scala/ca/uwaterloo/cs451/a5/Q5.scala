package ca.uwaterloo.cs451.a5

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.rogach.scallop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val text = opt[Boolean](descr = "the plain-text version of the data", required = false)
  val parquet = opt[Boolean](descr = "the parquet version of the data", required = false)
  verify()
}

object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q5Conf(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf) 
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
            .map(nation => (nation(1), nation.getInt(0)))
            .collectAsMap()
        val nationMap = sc.broadcast(nationTable)
        val us_key = nationMap.value("UNITED STATES")
        val ca_key = nationMap.value("CANADA")
        val nationTable2 = nationRDD
            .map(nation => (nation.getInt(0), nation.getString(1)))
        val customerTable = customerRDD
            .map(customer => (customer.getInt(0), customer.getInt(3)))
            .collectAsMap()
        val customerMap = sc.broadcast(customerTable)
        val orderTable = orderRDD
            .map(order => (order.getInt(0), order.getInt(1)))
            .filter(order => (customerMap.value(order._2) == us_key || customerMap.value(order._2) == ca_key))
        val nationMap2 = sc.broadcast(nationTable2.collectAsMap())
        
        lineitemRDD
            .map(line => (line.getInt(0), line.getString(10).substring(0, 7)))
            .cogroup(orderTable)
            .filter(_._2._2.size != 0)
            .flatMap(line => {
                val nationKey = customerMap.value(line._2._2.head)
                val nationName = nationMap2.value(nationKey)
                line._2._1.map(shipdate => ((nationKey, nationName, shipdate), 1))
            })
            .reduceByKey(_ + _)
            .sortBy(_._1)
            .collect()
            .foreach(tuple => println(tuple._1._1, tuple._1._2, tuple._1._3, tuple._2))
        
    } else if (args.text()) {
        val text = sc.textFile(input + "/lineitem.tbl")
        val nation = sc.textFile(input + "/nation.tbl")
        val orders = sc.textFile(args.input() + "/orders.tbl")
        val customers = sc.textFile(input + "/customer.tbl")
        val nationTable = nation.map(nation => (nation.split("\\|")(0).toInt, nation.split("\\|")(1).toString))
        val nationTable2 = nation.map(nation => (nation.split("\\|")(1), nation.split("\\|")(0).toInt))
                                 .collectAsMap()
        val nationMap2 = sc.broadcast(nationTable2)
        val us_key = nationMap2.value("UNITED STATES")
        val ca_key = nationMap2.value("CANADA")
        val customerTable = customers.map(customer => (customer.split("\\|")(0).toInt, customer.split("\\|")(3).toInt))
                                     .collectAsMap()
        val customerMap = sc.broadcast(customerTable)
        val orderTable = orders.map(order => (order.split("\\|")(0).toInt, order.split("\\|")(1).toInt))
                               .filter(order => (customerMap.value(order._2) == us_key || customerMap.value(order._2) == ca_key))
        val nationMap = sc.broadcast(nationTable.collectAsMap())
        
        text
            .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(10).substring(0, 7)))
            .cogroup(orderTable)
            .filter(_._2._2.size != 0)
            .flatMap({line => 
                val nationKey = customerMap.value(line._2._2.head)
                val nationName = nationMap.value(nationKey)
                
                line._2._1.map(shipdate => ((nationKey, nationName, shipdate), 1))
            })
            .reduceByKey(_ + _)
            .sortBy(_._1)
            .collect()
            .foreach(tuple => println(tuple._1._1, tuple._1._2, tuple._1._3, tuple._2))
    }
    
  }
}
