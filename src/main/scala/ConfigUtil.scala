import org.apache.commons.configuration.XMLConfiguration

class ConfigUtil(path: String) extends Serializable {
  val conf = new XMLConfiguration(path)

  def getConfigSetting(key: String, default: String): String ={
    if(conf != null)
      conf.getString(key)
    else
      default
  }

  var confPath:String=""
  def setConfPath(path:String): Unit ={
    confPath=path;
  }

  def getConfPath():String={
    confPath
  }

  /*
  redis host
   */
  val redisHost: String = getConfigSetting("redisHost", "")

  /*
  redis port
   */
  val redisPort: Int = getConfigSetting("redisPort", "").toInt

  /*
  redis timeout
   */
  val redisTimeout: Int = getConfigSetting("redisTimeout", "").toInt
}
