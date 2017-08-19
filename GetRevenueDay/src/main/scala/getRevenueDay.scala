/**
  * Created by jaymishr on 8/19/2017.
  * It will get  perday revenue of closed and completed order
  */
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.fs._
import com.typesafe.config.ConfigFactory

object GetRevenueDay {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutil")
    val app_conf = ConfigFactory.load()
    val conf = new SparkConf().setMaster(app_conf.getConfig(args(3)).getString("executionMode")).setAppName("GetRevenue")
    val sc = new SparkContext(conf)

    val orders_input_path = args(0)
    val orders_item_output_path = args(1)
    val output_path = args(2)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val orders_input_path_exist = fs.exists(new Path(orders_input_path))
    val orders_item_input_path_exist = fs.exists(new Path(orders_item_output_path))
    val output_path_exist = fs.exists(new Path(output_path))

    if ( !orders_input_path_exist || !orders_item_input_path_exist)
      {
        println("Input Does Not Exist")
      }


    if (output_path_exist)
      {
        fs.delete(new Path(output_path), true)

      }

    val ordersObj = sc.textFile(orders_input_path)
    val ordersFilterRec = ordersObj.filter(rec => rec.split(",").last == "CLOSED" || rec.split(",").last == "COMPLETE")
    val ordersTupledObj = ordersFilterRec.map(rec => (rec.split(",")(0).toInt,rec.split(",")(1)))

    val ordersItemObj = sc.textFile(orders_item_output_path)
    val OrdersItemTupled = ordersItemObj.map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
    val joinDataObj = ordersTupledObj.join(OrdersItemTupled)
    val filterData = joinDataObj.map(rec => rec._2)
    val finalData = filterData.reduceByKey((acc,n)=>acc+n)

    finalData.saveAsTextFile(output_path)
    sc.stop()

  }

}
