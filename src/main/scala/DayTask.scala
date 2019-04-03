import java.text.SimpleDateFormat
import java.util.{Calendar, TimerTask}

import com.hiklife.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._

class DayTask(_sc: SparkContext, _g2016conf: ConfigUtil, _broadList: Broadcast[List[Any]],_table:String) extends TimerTask{
  private val sc=_sc;
  private val g2016conf=_g2016conf
  private val broadcastList=_broadList
  private val table=_table
  override def run(): Unit ={
    val redisUtil = new RedisUtil(g2016conf.redisHost, g2016conf.redisPort, g2016conf.redisTimeout.toInt)
    redisUtil.connect()
    //获取当前redis内部所有的AP,ID,MAC按设备ID 时间分组统计的数据
    val cal= Calendar.getInstance()
    val time=if(table.equals(CommFunUtils.MONTH_TABLE)){
      if(cal.get(Calendar.DATE)==1){
        cal.add(Calendar.MONTH,-1)
        new SimpleDateFormat("yyyyMM").format(cal.getTime)
      }else{
        null
      }
    }else if(table.equals(CommFunUtils.HOUR_TABLE)){
      cal.add(Calendar.HOUR,-1)
      new SimpleDateFormat("yyyyMMddHH").format(cal.getTime)
    }else if(table.equals(CommFunUtils.DAY_TABLE)){
      cal.add(Calendar.DATE,-1)
      new SimpleDateFormat("yyyyMMdd").format(cal.getTime)
    }else if(table.equals(CommFunUtils.MINUTE_TABLE)){
      cal.add(Calendar.HOUR,-1)
      new SimpleDateFormat("yyyyMMddHHmm").format(cal.getTime)
    }else if(table.equals(CommFunUtils.YEAR_TABLE)){
      if(cal.get(Calendar.DATE)==1&&cal.get(Calendar.MONTH)==0){
        cal.add(Calendar.YEAR,-1)
        new SimpleDateFormat("yyyy").format(cal.getTime)
      }else{
        null
      }
    }else{
      null
    }

    if(time!=null){
      val divmins = redisUtil.jedis.keys(CommFunUtils.MINNAME + CommFunUtils.SPLIT + time+CommFunUtils.SPLIT+"*")
      CommFunUtils.executeByMin(sc, divmins.toList, broadcastList, table)
    }else{
      println("time is null")
    }
    redisUtil.close()
  }
}
