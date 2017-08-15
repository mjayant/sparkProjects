/**
  * Created by jaymishr on 8/15/2017.
  */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.fs._

import com.typesafe.config.ConfigFactory


object WordCountTab {

  def main(args: Array[String]): Unit = {

    val app_conf = ConfigFactory.load()
    System.setProperty("hadoop.home.dir", "C:\\winutil")
    val conf = new SparkConf().setAppName("Word Count tab").setMaster(app_conf.getConfig(args(2)).getString("executionMode"))
    val sc = new SparkContext(conf)

    val input_path = args(0)
    val output_psth = args(1)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val input_path_is_exist = fs.exists(new Path(input_path))
    val out_put_is_exist = fs.exists(new Path(output_psth))

    if (! input_path_is_exist)
      {
          println(" Input doesn't exist")

        return
      }

    if (out_put_is_exist)
      {
          fs.delete(new Path(output_psth), true)
      }
    val input = sc.textFile(input_path)
    val data = input.map(rec => (rec.split(',').last, 1))
    //val data = input_path.map(rec=> (rec.split(",").last, 1))
    val key_total_count = data.reduceByKey((acc,value)=> (acc+value))
    val final_value = key_total_count.map(rec => rec._1 + "\t" +rec._2)
    final_value.saveAsTextFile(output_psth)

    sc.stop()
  }


}
