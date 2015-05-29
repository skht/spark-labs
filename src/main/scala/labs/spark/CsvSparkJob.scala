package labs.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object CsvSparkJob {
  
  def main(args: Array[String]):Unit = {
    println(args.mkString(","))
    
    val conf = new SparkConf().setMaster("local").setAppName("My fantastic app")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val header = "id,name,link_id,valid,amount"
    val options_noheader = Map("header" -> "false", "path" -> "src/main/resources/test-file.csv")
    val options = Map("header" -> "true", "path" -> "src/main/resources/test-file.csv")
    
    val df_noheader = sqlContext.load("com.databricks.spark.csv", options_noheader)

    val csv = sqlContext.load("com.databricks.spark.csv", options)
    println("")
    println(" ** Iterating over DataFrame")
    println("")
    csv.foreach { x => println(x) }
    csv.printSchema
    csv.select("name")
    
    csv.registerTempTable("grades")
    println("")
    println(" ** SELECT * FROM Grades")
    println("")
    val results1 = sqlContext.sql("SELECT * FROM grades")
    results1.foreach { x => println(x) }
    
    println("")
    println(" ** SELECT * FROM Grades WHERE valid = 'Y'")
    println("")
    val results2 = sqlContext.sql("SELECT * FROM grades WHERE valid = 'Y'")
    results2.foreach { x => println(x) }

    println("")
    println(" ** SELECT sum(amount) FROM Grades WHERE valid = 'Y'")
    println("")
    val results3 = sqlContext.sql("SELECT sum(amount) FROM grades WHERE valid = 'Y'")
    println(s"Rows: ${results3.count()}")
    results3.foreach { x => println(x) }
    
    
  }
  
}
