package labs.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object BasicSparkJob {
  
  def main(args: Array[String]):Unit = {
    println(args.mkString(","))
    
    val conf = new SparkConf().setMaster("local").setAppName("My fantastic app")
    val sc = new SparkContext(conf)
    
    val input = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    input.map { x => x + 2 }.filter { x => x % 2 == 0 }.foreach { x => println(x) }
    
    println(s"Total: ${input.fold(0)((x,y) => x + pureEvenDouble(y))}")
    
    
    val complexInputDate = List(
      ("Y",0.5, 123),
      ("Y",0.5, 23),
      ("Y",0.5, 45),
      ("Y",0.75, 11),
      ("N",0.9, 99),
      ("Y",0.9, 1234),
      ("Y",0.1, 12),
      ("N",0.1, 67)
    )
    val zeroValue = ("T",0.0,0);
    val complexInput = sc.parallelize(complexInputDate, 2)
    complexInput.filter(_._1.equals("Y")).foreach(println(_))  
    
  }
  
  
  def pureEvenDouble(x:Int) = 
    if(x%2 == 0) x
    else x*x
    
  def pureComputation(condition:String, ratio:Double, amount:Double) =  
    condition match {
      case "Y" => amount * ratio
      case  _  => 0
    }
}
