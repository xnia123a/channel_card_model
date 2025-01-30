package channel_card_model

/**
 * Created by Marina on 2018/4/13.
 */

//   nohup /opt/spark2/bin/spark-submit --class channel_card_model.fake_user /home/yimr/rfz/IoT.jar 20180401

import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object fake_user {
  private val master = "xxxxxxxxxxx"
  private val port = "xxxxxxxxxxx"
  private val appName = "xxxxxxxxxxx"

  private val hdfs_path = "xxxxxxxxxxx"
  private val input_path = hdfs_path + "xxxxxxxxxxx"
  //private val businesschance_path = hdfs_path + "xxxxxxxxxxx"
  //private val input_last_month_path = hdfs_path + "xxxxxxxxxxx"
  private val data_output_sms = hdfs_path + "xxxxxxxxxxx"
  private val data_output_voice = hdfs_path + "xxxxxxxxxxx"
  private val data_output_data = hdfs_path + "xxxxxxxxxxx"


  case class wlw_user_info(cust_name: String, wlw_num: String)

  case class acct_snapshot(acct_cycle: String, acctID: String, account_name: String, operator_id: String,
                           account_status: String, account_billable_flag: String)

  case class Usage_Detail_SMS(acctID: String, ICCID: String, SIM_State:String,
                              Rateplan_id: Int, Ratezone_id:Int,
                              JPO_ACCT_SMS_NUM: Long)

  case class Usage_Detail_Data(acctID: String, ICCID: String, SIM_State:String,
                               Rateplan_id: Int, Ratezone_id:Int,
                               DataUsage_RawTotal:Int, DataUsage_RawUplink:Int,
                               DataUsage_RawDownlink:Int, DataUsage_RawRounded:Int)

  case class Usage_Detail_Voice(acctID: String, ICCID: String, SIM_State:String,
                                Rateplan_id: Int, Ratezone_id:Int,
                                VoiceDuration_Raw: Int,  VoiceDuration_Rounded:Int)



  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println( s"""
                           | <day> the date of data
    """.stripMargin)
      System.exit(1)
    }

    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(hdfs_path), new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(new Path(data_output_sms)))
      hdfs.delete(new Path(data_output_sms), true)

    
    if (hdfs.exists(new Path(data_output_voice)))
      hdfs.delete(new Path(data_output_voice), true)

    if (hdfs.exists(new Path(data_output_data)))
      hdfs.delete(new Path(data_output_data), true)

    val spark = SparkSession
      .builder
      .appName(appName)
      .config("spark.executor.memory", "100g")
      .config("spark.cores.max", "72")
      .config("spark.dynamicAllocation.enabled", "false")
      .master(s"spark://$master:$port")
      .getOrCreate()
    val sc = spark.sparkContext

    val day_logic = args(0)
    val days_period = cycle_date.get_logic2file_day(day_logic)
    //val day_sigle = args(1)

    val data_input_AcctSnapshot = input_path + "JWCC_" + day_logic + "_AcctSnapshot_*"
    val data_input_user = hdfs_path + "xxxxxxxxxxx"

    //
    var data_input_Usage_Detail_Data = spark.read.textFile(input_path + "JWCC_" + days_period + "_DataUsage_*")
    var data_input_Usage_Detail_SMS = spark.read.textFile(input_path + "JWCC_" + days_period + "_SMSUsage_*")
    var data_input_Usage_Detail_Voice = spark.read.textFile(input_path + "JWCC_" + days_period + "_VoiceUsage_*")

    /*
    //累计数据用量
    var data_input_Usage_Detail_Data = spark.read.textFile(input_path + "JWCC_" + days_period(0) + "_DataUsage_*")
    for (index <- 1 until days_period.length) {
      data_input_Usage_Detail_Data = data_input_Usage_Detail_Data.union(spark.read.textFile(input_path + "JWCC_" + days_period(index) + "_DataUsage_*"))
    }

    //累计短信用量
    var data_input_Usage_Detail_SMS = spark.read.textFile(input_path + "JWCC_" + days_period(0) + "_SMSUsage_*")
    for (index <- 1 until days_period.length) {
      data_input_Usage_Detail_SMS = data_input_Usage_Detail_SMS.union(spark.read.textFile(input_path + "JWCC_" + days_period(index) + "_SMSUsage_*"))
    }


    //累计语音用量
    var data_input_Usage_Detail_Voice = spark.read.textFile(input_path + "JWCC_" + days_period(0) + "_VoiceUsage_*")
    for (index <- 1 until days_period.length) {
      data_input_Usage_Detail_Voice = data_input_Usage_Detail_Voice.union(spark.read.textFile(input_path + "JWCC_" + days_period(index) + "_VoiceUsage_*"))
    }
    */
    import spark.implicits._
    //基本表acctSnapshot Done
    val acct_snapshot_df = spark.read.textFile(data_input_AcctSnapshot)
      .map(_.split("\\|", -1))
      .filter(splits => splits.length > 6)
      .map(splits => acct_snapshot(
      day_logic, splits(0), splits(1), splits(6),
      splits(2),
      splits(5)
    ))
      .filter("account_status = 'A' and account_billable_flag ='Y' and account_name like 'Company%'")
      //.agg(countDistinct("acctID").as("TOTAL_ACCT_NUMS"))
      .toDF()

    //val period = Util_date.get_account_period_2utc(day_logic)

    //Usage_Detail_SMS表
    val Usage_Detail_SMS_df = data_input_Usage_Detail_SMS
      .map(_.split("\\|", -1))
      .filter(splits => splits.length > 18)
      .map(splits => Usage_Detail_SMS(
      splits(4), splits(1), splits(7),splits(9).toInt,splits(10).toInt, 1
    ))
      .filter("acctID in ('xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx')")
      .filter("SIM_State ='6'")
      .toDF()


    //Usage_Detail_Data表
    val Usage_Detail_Data_df = data_input_Usage_Detail_Data
      .map(_.split("\\|", -1))
      .filter(splits => splits.length > 20)
      .map(splits => Usage_Detail_Data(
      splits(4), splits(1), splits(7),
      splits(9).toInt,splits(10).toInt,
      splits(13).toInt,splits(14).toInt,
      splits(15).toInt,splits(16).toInt
    )).toDF()
      //.filter("received_date >= '"+period(0)+"' and received_date < '"+period(1)+"'")
      .filter("DataUsage_RawRounded > 0 ")
      .filter("acctID in ('xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx')")
      .filter("SIM_State ='6'")

    //Usage_Detail_Voice表
    val Usage_Detail_Voice_df = data_input_Usage_Detail_Voice
      .map(_.split("\\|", -1))
      .filter(splits => splits.length > 18)
      .map(splits => Usage_Detail_Voice(
      splits(4), splits(1), splits(7),
      splits(9).toInt,splits(10).toInt,
      splits(15).toInt,splits(16).toInt
    )).toDF()
      //.filter("received_date >= '"+period(0)+"' and received_date < '"+period(1)+"'")
      .filter("VoiceDuration_Rounded > 0 ")
      .filter("acctID in ('xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx')")
      .filter("SIM_State ='6'")


    //账户短信总用量
    Usage_Detail_SMS_df.groupBy("acctID","ICCID")
      .agg(count(lit(1)).as("JPO_ACCT_SMS_NUM"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      //.repartition(1).rdd.saveAsTextFile(data_output)
      .createOrReplaceTempView("view_Usage_Detail_SMS")

    //账户数据总用量,,"SIM_State","Rateplan_id","Ratezone_id","Record_Received_Date"
    //DataUsage_RawTotal:Int, DataUsage_RawUplink:Int, DataUsage_RawDownlink:Int, DataUsage_Rounded:Int
    Usage_Detail_Data_df.groupBy("acctID","ICCID")
      .agg(sum("DataUsage_RawTotal").as("DataUsage_RawTotal"), sum("DataUsage_RawUplink").as("DataUsage_RawUplink"), sum("DataUsage_RawDownlink").as("DataUsage_RawDownlink"), sum("DataUsage_RawRounded").as("DataUsage_RawRounded"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_Usage_Detail_Data")

    //账户语音总用量 VoiceDuration_Raw: Int,  VoiceDuration_Rounded:Int
    Usage_Detail_Voice_df.groupBy("acctID","ICCID")
      .agg(sum("VoiceDuration_Raw").as("VoiceDuration_Raw"), sum("VoiceDuration_Rounded").as("VoiceDuration_Rounded"))
      //.agg(sum("VoiceDuration_Rounded").as("VoiceDuration_Rounded"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_Usage_Detail_Voice")

    //物联网user表,处理所有数据，不局限于被标定的3家窜卡用户
    val wlw_user_df = spark.read.textFile(data_input_user)
      .map(_.split("\\|", -1))
      .map(splits => wlw_user_info(splits(0), splits(7)))
      .filter("cust_name in ('xxxxxxxxxxx','xxxxxxxxxxx','xxxxxxxxxxx')")
      .toDF()

    Usage_Detail_SMS_df.createOrReplaceTempView("view_sms")
    Usage_Detail_Data_df.createOrReplaceTempView("view_data")
    Usage_Detail_Voice_df.createOrReplaceTempView("view_voice")

    acct_snapshot_df.createOrReplaceTempView("AcctSnapshot")
    wlw_user_df.createOrReplaceTempView("WLWUser")
    //Rateplan_id: Int, Ratezone_id:Int,
    spark.sql("SELECT VsmsFinal.acctID, VsmsFinal.ICCID, vsms.SIM_State, " +
      "vsms.Rateplan_id, vsms.Ratezone_id, VsmsFinal.JPO_ACCT_SMS_NUM " +
      "FROM view_Usage_Detail_SMS VsmsFinal left join view_sms vsms on VsmsFinal.ICCID = vsms.ICCID ")
      .createOrReplaceTempView("view_final_sms")

    spark.sql("SELECT VvoiceFinal.acctID, VvoiceFinal.ICCID, vvoice.SIM_State, " +
      "vvoice.Rateplan_id, vvoice.Ratezone_id, VvoiceFinal.VoiceDuration_Raw, VvoiceFinal.VoiceDuration_Rounded " +
      "FROM view_Usage_Detail_Voice VvoiceFinal left join view_voice vvoice on VvoiceFinal.ICCID = vvoice.ICCID ")
      .createOrReplaceTempView("view_final_voice")

    spark.sql("SELECT VdataFinal.acctID, VdataFinal.ICCID, vdata.SIM_State, " +
      "vdata.Rateplan_id, vdata.Ratezone_id, VdataFinal.DataUsage_RawTotal, " +
      "VdataFinal.DataUsage_RawUplink, VdataFinal.DataUsage_RawDownlink, VdataFinal.DataUsage_RawRounded "+
      "FROM view_Usage_Detail_Data VdataFinal left join view_data vdata on VdataFinal.ICCID = vdata.ICCID ")
      .createOrReplaceTempView("view_final_data")

    val out_df = spark.sql("SELECT DISTINCT(vdata.ICCID), " +
      "vas.acct_cycle, vdata.acctID, " +
      "vdata.DataUsage_RawTotal as DataUsage_RawTotal, "+
      "vdata.DataUsage_RawUplink as DataUsage_RawUplink, "+
      "vdata.DataUsage_RawDownlink as DataUsage_RawDownlink, "+
      "vdata.DataUsage_RawRounded as DataUsage_RawRounded, "+
      "vsms.JPO_ACCT_SMS_NUM as JPO_ACCT_SMS_NUM, "+
      "vvoice.VoiceDuration_Raw as VoiceDuration_Raw, "+
      "vvoice.VoiceDuration_Rounded as VoiceDuration_Rounded, "+
      "vsms.Rateplan_id as sms_Rateplan_id, " +
      "vsms.Ratezone_id as sms_Ratezone_id, " +
      "vvoice.Rateplan_id as voice_Rateplan_id, " +
      "vvoice.Ratezone_id as voice_Ratezone_id, " +
      "vdata.Rateplan_id as data_Rateplan_id, " +
      "vdata.Ratezone_id as data_Ratezone_id, " +
      "wu.cust_name "+

      "FROM view_final_data vdata left join view_final_sms vsms on vsms.ICCID = vdata.ICCID "+
      "left join view_final_voice vvoice on vvoice.ICCID = vdata.ICCID "+
      "left join AcctSnapshot vas on vas.acctID = vdata.acctID "+
      "left join WLWUser wu on wu.wlw_num = vas.operator_id "

    )
      .na.fill(Map(
      "DataUsage_RawTotal" -> "0",
      "DataUsage_RawUplink" -> "0",
      "DataUsage_RawDownlink" -> "0",
      "DataUsage_RawRounded" -> "0",
      "JPO_ACCT_SMS_NUM" -> "0",

      "VoiceDuration_Raw" -> "0",
      "VoiceDuration_Rounded"-> "0"

    ))


    val data_output = hdfs_path + "/home/yimr/rfz/output/test_fake_user/"+day_logic+"/"
    val data_output_day = hdfs_path + "/home/yimr/rfz/output/test_fake_user/"
    if (hdfs.exists(new Path(data_output)))
      hdfs.delete(new Path(data_output), true)
    out_df.repartition(1).rdd.saveAsTextFile(data_output)




    /*
    和保存mysql数据库一样，依旧会内存溢出
    out_df.repartition(1).write.option("header", "true").csv(data_output)
    val out_df_fileData = spark.read.option("header", "true").csv(data_output+"/part-00000")
    out_df_fileData.write.mode(SaveMode.Append).jdbc(mysqlDriverUrl, "iotoperation.test_xh_3", connectProperties)
    */

    //out_df.repartition(1).rdd.saveAsTextFile(data_output)


    val connectProperties = new Properties()
    connectProperties.put("user", "root")
    connectProperties.put("password", "root")
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val mysqlDriverUrl = "xxxxxxxxxxx"

   // out_df.write.mode(SaveMode.Append).jdbc(mysqlDriverUrl, "xxxxxxxxxxx", connectProperties)



  }
}