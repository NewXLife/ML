/**
  * create by colin on 2018/8/17
  */
object AddConf{
  import java.util.Properties
  import java.io.FileInputStream

  //conf-prod.properties 里的内容为"ddd=5.6,1.2"

  def loadProperties():Properties = {
    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource("conf-prod.properties").getPath //文件要放到resource文件夹下
    properties.load(new FileInputStream(path))
    println(properties.getProperty("ddd"))//读取键为ddd的数据的值
    println(properties.getProperty("ddd","没有值"))//如果ddd不存在,则返回第二个参数
    properties.setProperty("ddd","123")//添加或修改属性值
    properties
  }

  println(loadProperties().getProperty("ddd"))
}
