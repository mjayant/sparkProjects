/**
  * Created by jaymishr on 8/13/2017.
  */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.fs._

import com.typesafe.config.ConfigFactory

object WordCount {

  def main(args: Array[String]): Unit =
  {
    val appConf = ConfigFactory.load()
    println(args(0))
    println(args(1))
    println(args(2))
    System.setProperty("hadoop.home.dir", "C:\\winutil")
    val conf = new SparkConf().setAppName("Word Count").setMaster(appConf.getConfig(args(2)).getString("executionMode"))
    val sc = new SparkContext(conf)

    val input_path = args(0)
    val output_path = args(1)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPathExist = fs.exists(new Path(input_path))
    val outputPathExist = fs.exists(new Path(output_path))

    if( !inputPathExist) {
      println(" Input path does not exist")

      return

    }

    if(outputPathExist){

      fs.delete(new Path(output_path), true)
    }

    val input = sc.textFile(input_path)
    //val a1 = input.flatMap(rec=> (rec.split(" "), 1))
    val data_array = input.flatMap(rec=> rec.split(" "))
    val data_with_key = data_array.map(rec=> (rec, 1))
    val final_data = data_with_key.reduceByKey((acc, value)=> (acc+value))
    final_data.saveAsTextFile(output_path)

  }

}
