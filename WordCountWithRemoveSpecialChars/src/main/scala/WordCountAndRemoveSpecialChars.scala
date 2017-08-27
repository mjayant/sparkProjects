/**
  * Created by jaymishr on 8/27/2017.
  *Count the occurrence of each word and sort in descending order
Remove special characters and the endings "'s", "ly", "ed", "ing", "ness" and convert all words to lowercase
  */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.fs._
import com.typesafe.config.ConfigFactory

object WordCountAndRemoveSpecialChars {
  //Count the occurrence of each word and sort in descending order
  //Remove special characters and the endings "'s", "ly", "ed", "ing", "ness" and convert all words to lowercase
  def main(args: Array[String]): Unit = {
    val app_conf = ConfigFactory.load()
    val conf = new SparkConf().setMaster(app_conf.getConfig(args(2)).getString("executionMode")).setAppName("WordCout With Chars")
    val sc = new SparkContext(conf)
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val input_path = args(0)
    val output_path = args(1)
    val is_input_path_exist = fs.exists(new Path(input_path))
    val is_output_path_exist = fs.exists(new Path(output_path))

    if (!is_input_path_exist)
    {
      println("Input doesn't exist")
    }

    if (is_output_path_exist)
    {
      fs.delete(new Path(output_path), true)
    }

    val file = sc.textFile(input_path)
    val a1 = file.map(rec=> rec.replaceAll("[^a-zA-Z0-9]+ |('s|ly|ed|ing|ness)", ""))
    val a2 = a1.flatMap(rec=> rec.split(" "))
    val a3 = a2.map(rec=> (rec, 1))
    val a4 = a3.reduceByKey((acc, value)=> (acc+value))
    a4.saveAsTextFile(output_path)
    sc.stop()

  }

}
