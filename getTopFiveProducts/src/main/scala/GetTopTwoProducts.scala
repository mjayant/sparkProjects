/**
  * Created by jaymishr on 8/25/2017.
  */

import org.apache.spark.{SparkContext, SparkConf}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs._

object GetTopTwoProducts {

  // It get top two selling  produtcs by catagoris
  def main(args: Array[String]): Unit = {

    val app_conf = ConfigFactory.load()
    val conf = new SparkConf().setAppName("Get Products").setMaster(app_conf.getConfig(args(4)).getString("executionMode"))
    val sc = new SparkContext(conf)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val order_item_input_path = args(0)
    val products_input_path = args(1)
    val cat_input_path = args(2)
    val output_path = args(3)

    val order_item_is_input_path_exist = fs.exists(new Path(order_item_input_path))
    val products_is_input_path_exist = fs.exists(new Path(products_input_path))
    val cat_is_input_path_exist = fs.exists(new Path(cat_input_path))
    val is_out_put_path_exist = fs.exists(new Path(output_path))

    if (!order_item_is_input_path_exist || !products_is_input_path_exist || !cat_is_input_path_exist)
    {
      println("Input Doesn't exist")
    }

    if (is_out_put_path_exist)
    {
      fs.delete(new Path(output_path), true)
    }

    val order_items_file = sc.textFile(order_item_input_path)
    val oi_data = order_items_file.map(rec=> (rec.split(",")(2), rec.split(",")(4)))

    val pro_file = sc.textFile(products_input_path)
    val pro_data = pro_file.map(rec=> (rec.split(",")(0), (rec.split(",")(1), rec.split(",")(2))))
    val pro_ord_item_data = pro_data.join(oi_data)

    val cat_file = sc.textFile(cat_input_path)
    val cat_data= cat_file.map(rec=> (rec.split(",")(0).toInt, rec.split(",")(2)))
    val a1 = pro_ord_item_data.map(rec=> (rec._2._1._1.toInt, (rec._1.toInt, rec._2._1._2, rec._2._2.toFloat)))
    val a2 = cat_data.join(a1)
    val a3 = a2.map(rec=> (rec._1, (rec._2._1, rec._2._2._1, rec._2._2._2, rec._2._2._3)))
    val a4 = a3.map(rec=> ((rec._1, rec._2._1, rec._2._3), (rec._2._4)))
    val a5 = a4.groupBy(rec=> (rec._1._3,rec._1._1 ))
    val a6 = a5.map(rec=>(rec._1, rec._2.map(re=> re._2).sum))
    val a7 = a6.map(rec=> (rec._1._2, (rec._1._1, rec._2)))
    val a8 = a7.groupByKey()
    val a9 = a8.map(rec=> (rec._1, rec._2.toList.sortBy(re=> -re._2)))
    val a10 = a9.map(rec=> (rec._1, (rec._2).take(2)))
    a10.saveAsTextFile(output_path)
  }

}
