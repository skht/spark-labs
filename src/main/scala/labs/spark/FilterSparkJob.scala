package labs.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object FilterSparkJob extends App {

  println(args.mkString(","))
    
  val conf = new SparkConf().setMaster("local").setAppName("My fantastic filter app")
  val sc = new SparkContext(conf)

  val rdd = sc.textFile("TEST.DATA")
  rdd.map(x => x.split(';')).foreach(println)

  val keys = rdd.map(x => pureExtract(x))
  keys.groupByKey().foreach(println)

  val amsKeys = keys.filter(x => x._1 == "A").map(_._2)
  val eurKeys = keys.filter(x=> x._1 == "E").map(_._2)
  val pacKeys = keys.filter(x => x._1 == "P").map(_._2)
  amsKeys.saveAsTextFile("A.OUT")
  eurKeys.saveAsTextFile("E.OUT")
  pacKeys.saveAsTextFile("P.OUT")

  def pureExtract(input:String) = {
    val splitted = input.split(";")
    (splitted(3), input)
  }

}
