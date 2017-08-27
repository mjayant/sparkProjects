/**
  * Created by jaymishr on 8/27/2017.\
  * Find out the most viewed page
  */
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.fs._
import com.typesafe.config.ConfigFactory

object MostViewdPage {
//Find out the most viewed page

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
    val a1 = file.flatMap(rec=> rec.split(" "))
    val a2 =a1.filter(rec=> rec.contains("http://"))
    val a3 = a2.map(rec=> (rec, 1))
    val a4 = a3.reduceByKey((acc,value)=> (acc+value))
    val a5 = a4.sortBy(rec=> -rec._2)
    a5.saveAsTextFile(output_path)
    sc.stop()

  }

}
