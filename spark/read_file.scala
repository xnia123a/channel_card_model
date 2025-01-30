package channel_card_model

/**
 * Created by Marina on 2018/4/13.
 *
 * /opt/spark2/bin/spark-submit --class channel_card_model.read_file /home/yimr/rfz/IoT.jar
 */

//


import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


object read_file{
  private val master = "xxxxxxxxxxx"
  private val port = "xxxxxxxxxxx"
  private val appName = "xxxxxxxxxxx"

  private val hdfs_path = "xxxxxxxxxxx"
  private val input_path = hdfs_path + "xxxxxxxxxxx"
  //总表输出路径
//  private val data_output_fake_total = hdfs_path + "xxxxxxxxxxx"
//  private val data_output_normal_total = hdfs_path + "xxxxxxxxxxx"

  private val data_output_fake_total = hdfs_path + "xxxxxxxxxxx"
  private val data_output_normal_total = hdfs_path + "xxxxxxxxxxx"

  //单表数据输入路径
  private val data_output_fake_day = hdfs_path + "xxxxxxxxxxx"
  private val data_output_normal_day = hdfs_path + "xxxxxxxxxxx"

  case class fake_user(ICCID: String,
                       //acct_cycle:String,
                       acctID: String,
                       DataUsage_RawTotal: Long,
                       DataUsage_RawUplink: Long,
                       DataUsage_RawDownlink: Long,
                       DataUsage_RawRounded : Long,
                       JPO_ACCT_SMS_NUM: Long,
                       VoiceDuration_Raw:Long,
                       VoiceDuration_Rounded: Long,
                       sms_Rateplan_id: String,
                       sms_Ratezone_id :String,
                       voice_Rateplan_id: String,
                       voice_Ratezone_id:String,
                       data_Rateplan_id:String,
                       data_Ratezone_id: String,
                       cust_name:String)

  case class normal_user(ICCID: String,
                         //acct_cycle:String,
                         acctID: String,
                         DataUsage_RawTotal: Long,
                         DataUsage_RawUplink: Long,
                         DataUsage_RawDownlink: Long,
                         DataUsage_RawRounded : Long,
                         JPO_ACCT_SMS_NUM: Long,
                         VoiceDuration_Raw:Long,
                         VoiceDuration_Rounded: Long,
                         sms_Rateplan_id: String,
                         sms_Ratezone_id :String,
                         voice_Rateplan_id: String,
                         voice_Ratezone_id:String,
                         data_Rateplan_id:String,
                         data_Ratezone_id: String,
                         cust_name:String)

  def main(args: Array[String]): Unit = {


    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(hdfs_path), new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(new Path(data_output_normal_total)))
      hdfs.delete(new Path(data_output_normal_total), true)
    if (hdfs.exists(new Path(data_output_fake_total)))
      hdfs.delete(new Path(data_output_fake_total), true)


    val spark = SparkSession
      .builder
      .appName(appName)
      .config("spark.executor.memory", "100g")
      .config("spark.cores.max", "72")
      .config("spark.dynamicAllocation.enabled", "false")
      .master(s"spark://$master:$port")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //val date = List("20180201", "20180202", "20180203", "20180204", "20180205", "20180206", "20180207")
    val date = List("20180401", "20180402", "20180403", "20180404", "20180405", "20180406", "20180407")
    //val date = List("20180201")
    //增量日数据
    for (index <- 0 until date.length){
      var fake_user_data = spark.read.textFile(data_output_fake_day + date(index) + "/part-00000")
        .map(_.replace("[","").replace("]","").replace("null","0").split("\\,", -1))
        .filter(splits => splits.length > 15)
        .map(splits =>fake_user(
        splits(0), splits(2), splits(3).toLong, splits(4).toLong,
        splits(5).toLong, splits(6).toLong, splits(7).toLong, splits(8).toLong,
        splits(9).toLong, splits(10), splits(11), splits(12),
        splits(13), splits(14), splits(15),splits(16))).toDF()
        .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
        .createOrReplaceTempView("fake_" + date(index))


      //测试每日读取数据
      //      val test = spark.sql("SELECT * " +
      //        "FROM fake_20180201 "
      //     )
      //
      //      val connectProperties = new Properties()
      //      connectProperties.put("user", "root")
      //      connectProperties.put("password", "root")
      //      Class.forName("com.mysql.jdbc.Driver").newInstance()
      //      val mysqlDriverUrl = "xxxxxxxxxxx"
      //
      //
      //
      //      test.write.mode(SaveMode.Append).jdbc(mysqlDriverUrl, "iotoperation.test_fake_20180201", connectProperties)
      //

      var normal_user_data = spark.read.textFile(data_output_normal_day + date(index) + "/part-00000")
        .map(_.replace("[","").replace("]","").replace("null","0").split("\\,", -1))
        .filter(splits => splits.length > 15)
        .map(splits =>normal_user(
        splits(0), splits(2), splits(3).toLong, splits(4).toLong,
        splits(5).toLong, splits(6).toLong, splits(7).toLong, splits(8).toLong,
        splits(9).toLong, splits(10), splits(11), splits(12),
        splits(13), splits(14), splits(15),splits(16))).toDF()
        .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
        .createOrReplaceTempView("normal_" + date(index))

    }



    val fake_user_out = spark.sql("SELECT DISTINCT(f1.ICCID), " +
      "f1.data_Rateplan_id, " +
      "f1.data_Ratezone_id, " +
      "f1.cust_name, " +
      "f1.DataUsage_RawTotal as f1_DataUsage_RawTotal, " +
      "f1.DataUsage_RawUplink as f1_DataUsage_RawUplink, " +
      "f1.DataUsage_RawDownlink as f1_DataUsage_RawDownlink, "+
      "f1.DataUsage_RawRounded as f1_DataUsage_RawRounded, " +
      "f1.JPO_ACCT_SMS_NUM as f1_JPO_ACCT_SMS_NUM, " +
      "f1.VoiceDuration_Raw as f1_VoiceDuration_Raw, "+
      "f1.VoiceDuration_Rounded as f1_VoiceDuration_Rounded, " +

      "f2.DataUsage_RawTotal as f2_DataUsage_RawTotal, " +
      "f2.DataUsage_RawUplink as f2_DataUsage_RawUplink, " +
      "f2.DataUsage_RawDownlink as f2_DataUsage_RawDownlink, "+
      "f2.DataUsage_RawRounded as f2_DataUsage_RawRounded, " +
      "f2.JPO_ACCT_SMS_NUM as f2_JPO_ACCT_SMS_NUM, " +
      "f2.VoiceDuration_Raw as f2_VoiceDuration_Raw, "+
      "f2.VoiceDuration_Rounded as f2_VoiceDuration_Rounded, " +

      "f3.DataUsage_RawTotal as f3_DataUsage_RawTotal, " +
      "f3.DataUsage_RawUplink as f3_DataUsage_RawUplink, " +
      "f3.DataUsage_RawDownlink as f3_DataUsage_RawDownlink, "+
      "f3.DataUsage_RawRounded as f3_DataUsage_RawRounded, " +
      "f3.JPO_ACCT_SMS_NUM as f3_JPO_ACCT_SMS_NUM, " +
      "f3.VoiceDuration_Raw as f3_VoiceDuration_Raw, "+
      "f3.VoiceDuration_Rounded as f3_VoiceDuration_Rounded, " +

      "f4.DataUsage_RawTotal as f4_DataUsage_RawTotal, " +
      "f4.DataUsage_RawUplink as f4_DataUsage_RawUplink, " +
      "f4.DataUsage_RawDownlink as f4_DataUsage_RawDownlink, "+
      "f4.DataUsage_RawRounded as f4_DataUsage_RawRounded, " +
      "f4.JPO_ACCT_SMS_NUM as f4_JPO_ACCT_SMS_NUM, " +
      "f4.VoiceDuration_Raw as f4_VoiceDuration_Raw, "+
      "f4.VoiceDuration_Rounded as f4_VoiceDuration_Rounded, " +

      "f5.DataUsage_RawTotal as f5_DataUsage_RawTotal, " +
      "f5.DataUsage_RawUplink as f5_DataUsage_RawUplink, " +
      "f5.DataUsage_RawDownlink as f5_DataUsage_RawDownlink, "+
      "f5.DataUsage_RawRounded as f5_DataUsage_RawRounded, " +
      "f5.JPO_ACCT_SMS_NUM as f5_JPO_ACCT_SMS_NUM, " +
      "f5.VoiceDuration_Raw as f5_VoiceDuration_Raw, "+
      "f5.VoiceDuration_Rounded as f5_VoiceDuration_Rounded, " +

      "f6.DataUsage_RawTotal as f6_DataUsage_RawTotal, " +
      "f6.DataUsage_RawUplink as f6_DataUsage_RawUplink, " +
      "f6.DataUsage_RawDownlink as f6_DataUsage_RawDownlink, "+
      "f6.DataUsage_RawRounded as f6_DataUsage_RawRounded, " +
      "f6.JPO_ACCT_SMS_NUM as f6_JPO_ACCT_SMS_NUM, " +
      "f6.VoiceDuration_Raw as f6_VoiceDuration_Raw, "+
      "f6.VoiceDuration_Rounded as f6_VoiceDuration_Rounded, " +

      "f7.DataUsage_RawTotal as f7_DataUsage_RawTotal, " +
      "f7.DataUsage_RawUplink as f7_DataUsage_RawUplink, " +
      "f7.DataUsage_RawDownlink as f7_DataUsage_RawDownlink, "+
      "f7.DataUsage_RawRounded as f7_DataUsage_RawRounded, " +
      "f7.JPO_ACCT_SMS_NUM as f7_JPO_ACCT_SMS_NUM, " +
      "f7.VoiceDuration_Raw as f7_VoiceDuration_Raw, "+
      "f7.VoiceDuration_Rounded as f7_VoiceDuration_Rounded " +


//      "FROM fake_20180201 f1 inner join fake_20180202 f2 on f1.ICCID = f2.ICCID "+
//      "inner join fake_20180203 f3 on f1.ICCID = f3.ICCID "+
//      "inner join fake_20180204 f4 on f1.ICCID = f4.ICCID "+
//      "inner join fake_20180205 f5 on f1.ICCID = f5.ICCID "+
//      "inner join fake_20180206 f6 on f1.ICCID = f6.ICCID "+
//      "inner join fake_20180207 f7 on f1.ICCID = f7.ICCID "

      "FROM fake_20180401 f1 inner join fake_20180402 f2 on f1.ICCID = f2.ICCID "+
      "inner join fake_20180403 f3 on f1.ICCID = f3.ICCID "+
      "inner join fake_20180404 f4 on f1.ICCID = f4.ICCID "+
      "inner join fake_20180405 f5 on f1.ICCID = f5.ICCID "+
      "inner join fake_20180406 f6 on f1.ICCID = f6.ICCID "+
      "inner join fake_20180407 f7 on f1.ICCID = f7.ICCID "
    )

    if (hdfs.exists(new Path(data_output_fake_total)))
      hdfs.delete(new Path(data_output_fake_total), true)
    //fake_user_out.repartition(1).rdd.saveAsTextFile(data_output_fake_total)
    fake_user_out.repartition(1).write.option("header", "true").csv(data_output_fake_total)


    val normal_user_out = spark.sql("SELECT DISTINCT(n1.ICCID), " +
      "n1.data_Rateplan_id, " +
      "n1.data_Ratezone_id, " +
      "n1.cust_name, " +
      "n1.DataUsage_RawTotal as n1_DataUsage_RawTotal, " +
      "n1.DataUsage_RawUplink as n1_DataUsage_RawUplink, " +
      "n1.DataUsage_RawDownlink as n1_DataUsage_RawDownlink, "+
      "n1.DataUsage_RawRounded as n1_DataUsage_RawRounded, " +
      "n1.JPO_ACCT_SMS_NUM as n1_JPO_ACCT_SMS_NUM, " +
      "n1.VoiceDuration_Raw as n1_VoiceDuration_Raw, "+
      "n1.VoiceDuration_Rounded as n1_VoiceDuration_Rounded, " +

      "n2.DataUsage_RawTotal as n2_DataUsage_RawTotal, " +
      "n2.DataUsage_RawUplink as n2_DataUsage_RawUplink, " +
      "n2.DataUsage_RawDownlink as n2_DataUsage_RawDownlink, "+
      "n2.DataUsage_RawRounded as n2_DataUsage_RawRounded, " +
      "n2.JPO_ACCT_SMS_NUM as n2_JPO_ACCT_SMS_NUM, " +
      "n2.VoiceDuration_Raw as n2_VoiceDuration_Raw, "+
      "n2.VoiceDuration_Rounded as n2_VoiceDuration_Rounded, " +

      "n3.DataUsage_RawTotal as n3_DataUsage_RawTotal, " +
      "n3.DataUsage_RawUplink as n3_DataUsage_RawUplink, " +
      "n3.DataUsage_RawDownlink as n3_DataUsage_RawDownlink, "+
      "n3.DataUsage_RawRounded as n3_DataUsage_RawRounded, " +
      "n3.JPO_ACCT_SMS_NUM as n3_JPO_ACCT_SMS_NUM, " +
      "n3.VoiceDuration_Raw as n3_VoiceDuration_Raw, "+
      "n3.VoiceDuration_Rounded as n3_VoiceDuration_Rounded, " +

      "n4.DataUsage_RawTotal as n4_DataUsage_RawTotal, " +
      "n4.DataUsage_RawUplink as n4_DataUsage_RawUplink, " +
      "n4.DataUsage_RawDownlink as n4_DataUsage_RawDownlink, "+
      "n4.DataUsage_RawRounded as n4_DataUsage_RawRounded, " +
      "n4.JPO_ACCT_SMS_NUM as n4_JPO_ACCT_SMS_NUM, " +
      "n4.VoiceDuration_Raw as n4_VoiceDuration_Raw, "+
      "n4.VoiceDuration_Rounded as n4_VoiceDuration_Rounded, " +

      "n5.DataUsage_RawTotal as n5_DataUsage_RawTotal, " +
      "n5.DataUsage_RawUplink as n5_DataUsage_RawUplink, " +
      "n5.DataUsage_RawDownlink as n5_DataUsage_RawDownlink, "+
      "n5.DataUsage_RawRounded as n5_DataUsage_RawRounded, " +
      "n5.JPO_ACCT_SMS_NUM as n5_JPO_ACCT_SMS_NUM, " +
      "n5.VoiceDuration_Raw as n5_VoiceDuration_Raw, "+
      "n5.VoiceDuration_Rounded as n5_VoiceDuration_Rounded, " +

      "n6.DataUsage_RawTotal as n6_DataUsage_RawTotal, " +
      "n6.DataUsage_RawUplink as n6_DataUsage_RawUplink, " +
      "n6.DataUsage_RawDownlink as n6_DataUsage_RawDownlink, "+
      "n6.DataUsage_RawRounded as n6_DataUsage_RawRounded, " +
      "n6.JPO_ACCT_SMS_NUM as n6_JPO_ACCT_SMS_NUM, " +
      "n6.VoiceDuration_Raw as n6_VoiceDuration_Raw, "+
      "n6.VoiceDuration_Rounded as n6_VoiceDuration_Rounded, " +

      "n7.DataUsage_RawTotal as n7_DataUsage_RawTotal, " +
      "n7.DataUsage_RawUplink as n7_DataUsage_RawUplink, " +
      "n7.DataUsage_RawDownlink as n7_DataUsage_RawDownlink, "+
      "n7.DataUsage_RawRounded as n7_DataUsage_RawRounded, " +
      "n7.JPO_ACCT_SMS_NUM as n7_JPO_ACCT_SMS_NUM, " +
      "n7.VoiceDuration_Raw as n7_VoiceDuration_Raw, "+
      "n7.VoiceDuration_Rounded as n7_VoiceDuration_Rounded " +


//      "FROM normal_20180201 n1 inner join normal_20180202 n2 on n1.ICCID = n2.ICCID "+
//      "inner join normal_20180203 n3 on n1.ICCID = n3.ICCID "+
//      "inner join normal_20180204 n4 on n1.ICCID = n4.ICCID "+
//      "inner join normal_20180205 n5 on n1.ICCID = n5.ICCID "+
//      "inner join normal_20180206 n6 on n1.ICCID = n6.ICCID "+
//      "inner join normal_20180207 n7 on n1.ICCID = n7.ICCID limit 200000 "

    "FROM normal_20180401 n1 inner join normal_20180402 n2 on n1.ICCID = n2.ICCID "+
      "inner join normal_20180403 n3 on n1.ICCID = n3.ICCID "+
      "inner join normal_20180404 n4 on n1.ICCID = n4.ICCID "+
      "inner join normal_20180405 n5 on n1.ICCID = n5.ICCID "+
      "inner join normal_20180406 n6 on n1.ICCID = n6.ICCID "+
      "inner join normal_20180407 n7 on n1.ICCID = n7.ICCID limit 200000 "

    )

    if (hdfs.exists(new Path(data_output_normal_total)))
      hdfs.delete(new Path(data_output_normal_total), true)
    //normal_user_out.repartition(1).rdd.saveAsTextFile(data_output_normal_total)
    normal_user_out.repartition(1).write.option("header", "true").csv(data_output_normal_total)




        val connectProperties = new Properties()
        connectProperties.put("user", "root")
        connectProperties.put("password", "root")
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        val mysqlDriverUrl = "xxxxxxxxxxx"



        fake_user_out.write.mode(SaveMode.Append).jdbc(mysqlDriverUrl, "xxxxxxxxxxx", connectProperties)
        normal_user_out.write.mode(SaveMode.Append).jdbc(mysqlDriverUrl, "xxxxxxxxxxx", connectProperties)



  }
}