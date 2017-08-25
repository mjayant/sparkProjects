/**
  * Created by jaymishr on 8/25/2017.
  * Develop topNStocks function - function should take  parameters
  Get top N stocks by volume(5th cloumn of csv file) for each day
  First Parameter - topN
  Function should sort data in descending order and return top N stocks

  */

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.hadoop.fs._
import com.typesafe.config.ConfigFactory


object GetTopNStocks {
  def main(args: Array[String]): Unit = {
    val app_conf = ConfigFactory.load()
    val conf = new SparkConf().setAppName("NYSE project").setMaster(app_conf.getConfig(args(3)).getString("executionMode"))
    val sc = new SparkContext(conf)
    val input_path = args(0)
    val output_path = args(1)
    val top_n = args(2).toInt
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val input_path_exist = fs.exists(new Path(input_path))
    val output_path_exist = fs.exists(new Path(output_path))
    if (!input_path_exist)
      {
        println("Input doesn't exist")
      }
    if (output_path_exist)
      {
        fs.delete(new Path(output_path), true)
      }

    val file = sc.textFile(input_path)
    val a1 = file.map(rec=> (rec.split(",")(1), (rec.split(",")(0), rec.split(",")(5).toFloat)))
    val a2 = a1.groupByKey()
    val a3 = a2.map(rec=> (rec._1 , rec._2.toList.sortBy(re=> -re._2)))
    val a4 = a3.map(rec=> (rec._1,rec._2.take(top_n)))
    a4.saveAsTextFile(output_path)


  }
}
