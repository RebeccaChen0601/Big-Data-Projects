package ca.uwaterloo.cs451.a5

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.rogach.scallop._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


class Q3Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "the plain-text version of the data", required = false)
  val parquet = opt[Boolean](descr = "the parquet version of the data", required = false)
  verify()
}

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q3Conf(argv)

    log.info("Input: " + args.input())
    log.info("Ship date: " + args.date())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)
    val date = args.date()
    val input = args.input()

    if (args.parquet()){
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(input + "/lineitem")
        val partDF = sparkSession.read.parquet(input + "/part")
        val supplierDF = sparkSession.read.parquet(input + "/supplier")
        val lineitemRDD = lineitemDF.rdd
        val partRDD = partDF.rdd
        val supplierRDD = supplierDF.rdd
        val parts = partRDD
            .map(part => (part.getInt(0), part.getString(1)))
        val suppliers = supplierRDD
            .map(supplier => (supplier.getInt(0), supplier.getString(1)))
        val partMap = sc.broadcast(parts.collectAsMap())
        val supplyMap = sc.broadcast(suppliers.collectAsMap())
        lineitemRDD
            .filter(line => line.getString(10).contains(date))
            .map(line => (line.getInt(0), (partMap.value(line.getInt(1)), supplyMap.value(line.getInt(2)))))
            .sortBy(_._1)
            .take(20)
            .foreach(tuple => println(tuple))
        
    } else if (args.text()) {
        val text = sc.textFile(input + "/lineitem.tbl")
        val parts = sc.textFile(input + "/part.tbl")
        val suppliers = sc.textFile(input + "/supplier.tbl")
        val partTable = parts.map(part => (part.split("\\|")(0).toInt, part.split("\\|")(1).toString))
        val supplyTable = suppliers.map(supply => (supply.split("\\|")(0).toInt, supply.split("\\|")(1).toString))
        val partMap = sc.broadcast(partTable.collectAsMap())
        val supplyMap = sc.broadcast(supplyTable.collectAsMap())
        text
            .filter(line => line.split("\\|")(10).contains(date))
            .map(line => (line.split("\\|")(0).toInt, (partMap.value(line.split("\\|")(1).toInt), supplyMap.value(line.split("\\|")(2).toInt))))
            .sortBy(_._1)
            .take(20)
            .foreach(tuple => println(tuple))
    }
    
  }
}