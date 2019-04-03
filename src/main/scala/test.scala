import java.text.SimpleDateFormat
import java.util.Calendar

object test {
  def main(args: Array[String]): Unit = {
     val cal=Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    val m=new SimpleDateFormat("yyyyMMddHH").format(cal.getTime)
    println(m+"===>")
    val t=cal.get(Calendar.MONTH)
    println(t+"-----")
  }
}
