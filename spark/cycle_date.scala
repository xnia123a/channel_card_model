package channel_card_model

/**
 * Created by Marina on 2018/4/13.
 */
import java.sql
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Created by sss on 2018/2/6.
 */
object cycle_date {

  def str2date(str: String, pattern: String = "yyyyMMdd"): sql.Date = {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(str)
    new sql.Date(input_date.getTime)
  }

  def main(args: Array[String]) {

    println(get_logic2file_day("20180201"))

    println(get_account_period_day_list("20180305"))

    println(get_account_period_2utc("20180305"))

    println(get_file_month_last_day("20180305"))

    println("Hello\u0009World\n\n" )
    println("Hello\t\tWorld\n\n" )
    println("Hello"+"\u0009"+"World" )

    println(str2date("2018-02-06","yyyy-MM-dd"))



  }

  /*
    //获取单天数据源文件日期
    def get_sigle_day(sigle_day:String, pattern: String = "yyyyMMdd"):List[String]={
      val sdf = new SimpleDateFormat(pattern)
      val input_date = sdf.parse(sigle_day)
      val input_calendar = Calendar.getInstance()
      input_calendar.setTime(input_date)

      val month = input_calendar.get(Calendar.MONTH)

      var days: List[String] = List(sdf.format(input_calendar.getTime))

      while (true) {
        input_calendar.add(Calendar.DAY_OF_YEAR, -1)
        if (input_calendar.get(Calendar.MONTH) == month) {
          days = days :+ sdf.format(input_calendar.getTime)
        }else {
          return days
        }
      }
      return days

    }
    //获取单天utc的fileter条件日期，如20180301000000<=,<20180302000000
    def get_sigle_utc(sigle_utc:String, pattern: String = "yyyyMMdd", outPutPattern: String = "yyyyMMddHHmmss"):List[String]={
      val sdf = new SimpleDateFormat(pattern)
      val input_date = sdf.parse(sigle_utc)
      val input_calendar = Calendar.getInstance()
      input_calendar.setTime(input_date)

      val last_day = Calendar.getInstance()
      last_day.setTime(input_date)
      last_day.set(Calendar.DAY_OF_MONTH, 27)
      last_day.add(Calendar.HOUR, -8)

      val out_sdf = new SimpleDateFormat(outPutPattern)
      var days: List[String] = List(out_sdf.format(last_day.getTime))

      days = days :+ out_sdf.format(input_calendar.getTime)
      days


    }

  */


  /**
   *
   * @param day_logic  逻辑日期
   * @param pattern 文件日期
   * @return 文件比逻辑日期多一天
   */
  def get_logic2file_day(day_logic: String,  pattern: String = "yyyyMMdd") = {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day_logic)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    input_calendar.add(Calendar.DAY_OF_YEAR, +1)

    sdf.format(input_calendar.getTime)

  }

  /**

  以   上月27，到本月26 为一个账期

  根据输入日期获取上个账期最后一天
    文件名最后一天，比实际多一天

    * */
  def get_file_month_last_day(day: String,  pattern: String = "yyyyMMdd") = {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) < 31) {

    } else{
      input_calendar.add(Calendar.MONTH, -1)
    }
    input_calendar.set(Calendar.DAY_OF_MONTH, 26)

    val days = sdf.format(input_calendar.getTime)
    days
  }

  /**
   * 获取本月，到今天为止 所有的天
   * */
  def get_sofar_month_day(day: String, pattern: String = "yyyyMMdd") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    val month = input_calendar.get(Calendar.MONTH)

    var days: List[String] = List(sdf.format(input_calendar.getTime))

    while (true) {
      input_calendar.add(Calendar.DAY_OF_YEAR, -1)
      if (input_calendar.get(Calendar.MONTH) == month) {
        days = days :+ sdf.format(input_calendar.getTime)
      }else {
        return days
      }
    }
    return days
  }

  /**
  以   上月27，到本月26 为一个账期，获取一个账期内的所有天
    如查询日20180305，源文件名称为20180227-20180306
    * @param day
    * @param pattern
    * @return
    */
  def get_account_period_day_list(day: String, pattern: String = "yyyyMMdd") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)
    //取得账期开始时间
    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    last_day.set(Calendar.DAY_OF_MONTH, 27)
    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) < 31) {

    } else{
      last_day.add(Calendar.MONTH, -1)
    }

    //获取账期内，源文件最新日期，
    input_calendar.add(Calendar.DAY_OF_YEAR, +1)
    sdf.format(input_calendar.getTime)

    var days: List[String] = List(sdf.format(input_calendar.getTime))

    while (true) {
      input_calendar.add(Calendar.DAY_OF_YEAR, -1)
      if (input_calendar.compareTo(last_day) >= 0 ) {
        days = days :+ sdf.format(input_calendar.getTime)
      }else {
        return days
      }
    }
    days
  }

  /**
  获取一个账期的周期：（以utc时间返回）
          2月19号：返回：20180128,20180227
          2月27号：返回：20180128,20180227
          2月28号：返回：20180228,20180327
    * @param day
    * @param pattern
    * @return
    */
  def get_account_period_2utc(day: String, pattern: String = "yyyyMMdd", outPutPattern: String = "yyyyMMddHHmmss") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    last_day.set(Calendar.DAY_OF_MONTH, 27)
    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) < 31) {

    } else{
      last_day.add(Calendar.MONTH, -1)
    }

    last_day.add(Calendar.HOUR, -8)

    val out_sdf = new SimpleDateFormat(outPutPattern)
    var days: List[String] = List(out_sdf.format(last_day.getTime))
    //last_day.add(Calendar.MONTH, +1)
    //days = days :+ out_sdf.format(last_day.getTime)

    /*
    input_calendar.add(Calendar.HOUR,15)
    input_calendar.add(Calendar.MINUTE,59)
    input_calendar.add(Calendar.SECOND,59)
    input_calendar.add(Calendar.HOUR,-8)
    */
    input_calendar.add(Calendar.HOUR,+16)
    days = days :+ out_sdf.format(input_calendar.getTime)
    days
  }

}

