/**
  * Created by jaymishr on 8/20/2017.
  * it sort all order by status
  * All inputs are in input folder
  */
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.fs._
import com.typesafe.config.ConfigFactory


object OrderSortedByStatus {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutil")
    val app_conf = ConfigFactory.load()
    val conf = new SparkConf().setMaster(app_conf.getConfig(args(2)).getString("executionMode")).setAppName("sorted by status")
    val sc = new SparkContext(conf)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val input_path = args(0)
    val output_path = args(1)

    val is_input_path_exist = fs.exists( new Path(input_path))
    val is_output_path_exist = fs.exists(new Path(output_path))

    if (!is_input_path_exist)
      {
        println("Input Does Not Exist")
      }
    if (is_output_path_exist)
      {
        fs.delete(new Path(output_path), true)
      }

    val file_obj = sc.textFile(input_path)
    val keydata = file_obj.map(rec=> (rec.split(",")(3),( rec.split(",")(0),rec.split(",")(1),rec.split(",")(2))))
    val sortdata = keydata.sortByKey()
    sortdata.saveAsTextFile(output_path)
    sc.stop()

  }

}
