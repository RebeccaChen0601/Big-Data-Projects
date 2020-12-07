package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

class RegionEventCountConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object RegionEventCount {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new RegionEventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("RegionEventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val goldman = List((-74.0141012, 40.7152191), (-74.013777, 40.7152275), (-74.0141027, 40.7138745), (-74.0144185, 40.7140753))
    val citigroup = List((-74.011869, 40.7217236), (-74.009867, 40.721493), (-74.010140, 40.720053), (-74.012083, 40.720267))

    val goldman_x_min = goldman.map(x => x._1).toList.min
    val goldman_x_max = goldman.map(x => x._1).toList.max
    val goldman_y_min = goldman.map(x => x._2).toList.min
    val goldman_y_max = goldman.map(x => x._2).toList.max

    val citigroup_x_min = citigroup.map(x => x._1).toList.min
    val citigroup_x_max = citigroup.map(x => x._1).toList.max
    val citigroup_y_min = citigroup.map(x => x._2).toList.min
    val citigroup_y_max = citigroup.map(x => x._2).toList.max

    val wc = stream.map(_.split(","))
      .map(tuple => {
          if (tuple(0) == "yellow"){
              if (tuple(10).toDouble > goldman_x_min && tuple(10).toDouble < goldman_x_max && tuple(11).toDouble > goldman_y_min && tuple(11).toDouble < goldman_y_max) {
                ("goldman", 1)
              } else if (tuple(10).toDouble > citigroup_x_min && tuple(10).toDouble < citigroup_x_max && tuple(11).toDouble > citigroup_y_min && tuple(11).toDouble < citigroup_y_max) {
                ("citigroup", 1)
              } else {
                ("null", 0)
              }
          } else {
              if (tuple(8).toDouble > goldman_x_min && tuple(8).toDouble < goldman_x_max && tuple(9).toDouble > goldman_y_min && tuple(9).toDouble < goldman_y_max) {
                ("goldman", 1)
              } else if (tuple(8).toDouble > citigroup_x_min && tuple(8).toDouble < citigroup_x_max && tuple(9).toDouble > citigroup_y_min && tuple(9).toDouble < citigroup_y_max) {
                ("citigroup", 1)
              } else {
                ("null", 0)
              }
          } 
      })
      .filter(tuple => (tuple._1 != "null"))
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(60), Minutes(60))
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}