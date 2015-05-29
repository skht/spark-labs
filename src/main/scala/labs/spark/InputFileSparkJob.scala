package labs.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

object InputFileSparkJob {
  
  def main(args: Array[String]):Unit = {
    println(args.mkString(","))
    
    val conf = new SparkConf().setMaster("local").setAppName("InputFileJob")
    val sc = new SparkContext(conf)
    
    val input = sc.textFile("src/main/resources/test-file.csv")
    input.foreach { x => println(x) }
    val total = input.count
    println(s"All records :: $total")
  }
  
}
