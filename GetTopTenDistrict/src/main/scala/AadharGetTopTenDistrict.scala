/**
  * Created by jaymishr on 8/26/2017.
  *CSV file contains following columns: Registrar, Enrollment Agency, State, District, Sub District, Pin Code, Gender, Age, Aadhaar Generated, Enrollment Rejected, Residents providing email, Residents providing mobile number
  Top 10 districts with maximum identities generated for both Male and Female
  */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.fs._
import com.typesafe.config.ConfigFactory

object AadharGetTopTenDistrict {
  //Top 10 districts with maximum identities generated for both Male and Female
  def main(args: Array[String]): Unit = {
    val app_conf = ConfigFactory.load()
    val conf = new SparkConf().setMaster(app_conf.getConfig(args(2)).getString("executionMode")).setAppName("count Identities")
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
    val data = file.mapPartitionsWithIndex((index, iter)=> if (index==0) iter.drop(1) else iter)
    val a1 = data.map(rec=> ((rec.split(",")(2), rec.split(",")(3)),(rec.split(",")(8).toFloat)))
    val a2 = a1.groupByKey()
    val a3 = a2.map(rec=> (rec._1, rec._2.toList.sum))
    //val a3 = a2.map(rec=> (rec._1 , rec._2.toList.sum))
    //val a4 = a3.sortBy(rec=> -rec._2)
    val a4 = a3.sortBy(rec=> -rec._2)
    val a5 = a4.take(10)
    sc.parallelize(a5).saveAsTextFile(output_path)
    sc.stop()

  }

}
