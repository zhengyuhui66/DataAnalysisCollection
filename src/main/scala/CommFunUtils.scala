import java.text.SimpleDateFormat
import java.util.{Date, GregorianCalendar}

import com.hiklife.utils.{ByteUtil, HBaseUtil, RedisUtil}
import net.sf.json.{JSONArray, JSONObject}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object CommFunUtils  extends Serializable{

  val ENTER:String="1"
  val EXIT:String="0"
  val AP="ap"
  val MAC="mac"
  val ID="id"
  val MINNAME="devmin"
  val SPLIT="_"
  val HOUR="HOUR"
  val DAY="DAY"
  val MINUTE="MINUTE"
  val MINUTE_TABLE="MACDevMinuteTotal"
  val HOUR_TABLE="MACDevHourTotal"
  val DAY_TABLE="MACDevDayTotal"
  val MONTH_TABLE="MACDevMonthTotal"
  val YEAR_TABLE="MACDevYearTotal"

  def byte2HexStr(b:Byte):String= {
    val stmp = (b & 0xFF).toHexString.toUpperCase
     if (stmp.length == 1) {
      "0" + stmp
    } else {
       stmp
    }
  }

  def GetHashCodeWithLimit(context: String, limit: Int): Int =  {
    var hash = 0
    for (item <- context.getBytes)  {
      hash = 33 * hash + item
    }
    return (hash % limit)
  }


  def byte2HexStr(b: Array[Byte]): String =  { var hs: String = ""
    var stmp: String = ""
    var n: Int = 0
    for(i<-0 until b.length){
      stmp=(b(i)&0XFF).toHexString
      if(stmp.length==1){
        hs=hs+"0"+stmp
      }else{
        hs=hs+stmp
      }
    }
    return hs.toUpperCase
  }
  /**
    * 字符串(YYYY-MM-DD hh:mm:ss)转换成Date
    *
    * @param s
    * @return
    */
  def Str2Date(s: String): Date ={

    if (!(s == null || (s.equals("")))) try {
      val gc = new GregorianCalendar
      gc.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(s))
      gc.getTime
    } catch {
      case e: Exception =>{
        print(e)
        null
      }
    }
    else null
  }

  def getNowDate():String={
    val s:SimpleDateFormat=new SimpleDateFormat("yyyyMMdd")
    s.format(new Date())
  }

  //根据采集时间获取
  def getMinNowDate():String={
    val s:SimpleDateFormat=new SimpleDateFormat("yyyyMMddHHmm")
    s.format(new Date())
  }

  /**
    * 分钟按此统计，每小时运行一次
    *
    * @param sc
    * @param d
    * @param broadList
    */
  def executeByMin(sc:SparkContext,d:List[String],broadList:Broadcast[List[Any]],_table:String): Unit ={
    //将数据转成RDD内部
    sc.parallelize(d).mapPartitions(x=>{
      val list = new ListBuffer[(String,String)]()
      val host=broadList.value(0).toString
      val port=broadList.value(1).toString.toInt
      val timeout=broadList.value(2).toString.toInt
      val _redisUtil = new RedisUtil(host,port,timeout)
      _redisUtil.connect()
      while(x.hasNext){
        val rowkey=x.next();

        val value=_redisUtil.jedis.get(rowkey)
        list.add((rowkey,value))
      }

      _redisUtil.close()
      list.iterator
    }).map(x=>{
      var rows=x._1.split(CommFunUtils.SPLIT)
      //redis中的有效数据
      val devmin=rows(0)
      //redis中的时间分类
      val times=rows(1)
      //redis中的有效分类
      val types=rows(2)
      //redis中的devID分类
      val devId=rows(3)
      val devid=devId.substring(9)
      if(types.equals(CommFunUtils.ID)){
        val idtype=rows(4)
        (getkeyrow(devid)+devid+times,types+CommFunUtils.SPLIT+x._2+CommFunUtils.SPLIT+idtype)
      }else{
        (getkeyrow(devid)+devid+times,types+CommFunUtils.SPLIT+x._2)
      }

    }).reduceByKey(_+","+_).foreachPartition(partitionOfRecords=>{
      //路径
      val _path=broadList.value(3).toString
      val conn = ConnectionFactory.createConnection(HBaseUtil.getConfiguration(_path + "hbase/hbase-site.xml"))
      val table = conn.getTable(TableName.valueOf(_table)).asInstanceOf[HTable]
      table.setAutoFlush(false, false)
      table.setWriteBufferSize(5 * 1024 * 1024)
      partitionOfRecords.foreach(record =>{
        val key=record._1
        val values=record._2.split(",")
        //总输出
        val jsonVal=new JSONObject();
        //按ID类型 统计总数
        val idTypeJSONArray=new JSONArray();
        //ID统计总数
        var idCount=0;

        for(m<-values){
          val tmpVal=m.split(CommFunUtils.SPLIT)
          if(tmpVal(0).equals(CommFunUtils.ID)){
            val idtyJSON=new JSONObject()
            idtyJSON.accumulate("ty",tmpVal(2))
            idtyJSON.accumulate("id",tmpVal(1))
            idCount=idCount+tmpVal(1).toInt
            idTypeJSONArray.add(idtyJSON)
          }else{
            jsonVal.accumulate(tmpVal(0),tmpVal(1))
          }
        }
        jsonVal.accumulate(CommFunUtils.ID,idCount)
        jsonVal.accumulate("lstid",idTypeJSONArray.toString)

        val rowkey= new Put(key.getBytes);
        rowkey.addColumn("S".getBytes, "C".getBytes, jsonVal.toString.getBytes)
        table.put(rowkey)
      })
      table.flushCommits()
      table.close()
      conn.close()
    })
  }

  def getkeyrow(devid:String):String = CommFunUtils.byte2HexStr(CommFunUtils.GetHashCodeWithLimit(devid, 0xFF).asInstanceOf[Byte])

}
