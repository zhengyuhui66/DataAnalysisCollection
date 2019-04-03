import java.util
import java.util.Timer

import com.hiklife.utils.{HBaseUtil, RedisUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}


object App {
  def main(args: Array[String]):Unit={
    val path=args(0)
    val conf = new SparkConf().setAppName("DataAnalysisCollection")
    //conf.setMaster("local")
    val sc=new SparkContext(conf)
    val g2016conf = new ConfigUtil(path+"dataAnalysis/redis.xml")
       g2016conf.setConfPath(path)
    val hBaseUtil = new HBaseUtil(path+"hbase/hbase-site.xml")
    hBaseUtil.createTable(CommFunUtils.MINUTE_TABLE,"S")
    hBaseUtil.createTable(CommFunUtils.HOUR_TABLE,"S")
    hBaseUtil.createTable(CommFunUtils.DAY_TABLE,"S")
    hBaseUtil.createTable(CommFunUtils.MONTH_TABLE,"S")
    hBaseUtil.createTable(CommFunUtils.YEAR_TABLE,"S")
    val broadList: Broadcast[List[Any]] = sc.broadcast(List(g2016conf.redisHost, g2016conf.redisPort, g2016conf.redisTimeout, g2016conf.confPath));
     val timer:Timer = new Timer();
    //分钟粒度每半个小时执行一次
    timer.schedule(new DayTask(sc, g2016conf,broadList,CommFunUtils.MINUTE_TABLE),100l,3600*1000)
    //小时粒度每一个小时执行一次
    timer.schedule(new DayTask(sc, g2016conf,broadList,CommFunUtils.HOUR_TABLE),100l,3600*1000)
    //天粒度度每天执行一次
    timer.schedule(new DayTask(sc, g2016conf,broadList,CommFunUtils.DAY_TABLE),100l,24*3600*1000)
    //月粒度每天执行一次，判断是否为每月的1号，如果是每月的一号
    timer.schedule(new DayTask(sc, g2016conf,broadList,CommFunUtils.MONTH_TABLE),100l,24*3600*1000)
    //年粒度每年执行一次，判断是否为每年1月的1号，
    timer.schedule(new DayTask(sc, g2016conf,broadList,CommFunUtils.YEAR_TABLE),100l,24*3600*1000)
  }
}
