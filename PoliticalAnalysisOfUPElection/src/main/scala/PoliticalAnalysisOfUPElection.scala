/**
  * Created by jaymishr on 8/19/2017.
  * Problem Statement

  In 4 way contest BJP won almost 90% of seats in general elections
  If it was 3 way contest as UP state elections 2017, then how many seats BJP would have won, how many seats SP+INC should have won.
  Input is inside src/input folder
  There are ~8K records ,data is tab separated
  First record give the details about data, discard it while processing
  Data has details about all Lok Sabha constituencies in India, we are only interested in UP
  There are multiple records for each constituency, each record have details about state, constituency, candidate name, party, number of votes he got
  Get the votes of BJP, BSP and cumulative votes of INC+SP
  Final outcome, number of seats BJP, BSP and INC+SP assuming alliance
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs._
import com.typesafe.config.ConfigFactory


object PoliticalAnalysisOfUPElection {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutil")
    val app_conf = ConfigFactory.load()
    val conf = new SparkConf().setAppName("political analysis").setMaster(app_conf.getConfig(args(2)).getString("executionMode"))
    val sc = new SparkContext(conf)
    val input_path = args(0)
    val output_path = args(1)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val is_input_path_exist = fs.exists(new Path(input_path))
    val is_output_path_exist = fs.exists(new Path(output_path))

    if ( !is_input_path_exist )
      {
        println("Input doesn't exist")
      }

    if (is_output_path_exist)
      {
        fs.delete(new Path(output_path), true)
      }

    val file_obj = sc.textFile(input_path)
    val file_data = file_obj.mapPartitionsWithIndex((idx, iter)=> if (idx==0) iter.drop(1) else iter)
    val filter_data =  file_data.filter(rec=> rec.split("\t")(0)== "Uttar Pradesh")
    val data_with_ally = filter_data.map(rec=>
      if (rec.split("\t")(6)== "INC" || rec.split("\t")(6)=="SP")
      {
        ((rec.split("\t")(0), rec.split("\t")(1)), ("ALLY", rec.split("\t")(10)))
      }
      else{
        ((rec.split("\t")(0), rec.split("\t")(1) ), (rec.split("\t")(6), rec.split("\t")(10)))
      }
    )

    val gbkdata = data_with_ally.groupByKey()
    val gbdata = gbkdata.map(rec=> (rec._1, rec._2.groupBy(re=> re._1)))
    val a5 = gbdata.map(rec=> (rec._1, rec._2.mapValues(re=> re.map(ve=> ve._2.toInt).sum)))
    val a6 = a5.map(rec=> (rec._1, (rec._2.maxBy(re=>re._2))))
    a6.saveAsTextFile(output_path)
    sc.stop()
  }

}
