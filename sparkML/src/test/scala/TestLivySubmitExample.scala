import java.io.File
import java.net.URI

import beginners_guide_scala.TrySuccessFailureTest
import org.apache.livy.LivyClientBuilder

object TestLivySubmitExample extends App {


  val client = new LivyClientBuilder()
    .setURI(new URI("http://172.16.37.121:8998")) //load livy_url
    .build()

  try {
    System.err.printf("Uploading %s to the Spark context...\n", "tt");
    System.out.println("Uploading livy-example jar to the SparkContext...");


    val ss = System.getProperty("java.class.path").split(File.pathSeparator)


    ss.foreach { x => {
      if (new File(x).getName.startsWith("Example")) {
        System.out.printf("find jar:%s \n", new File(x).getName)
        client.uploadJar(new File(x)).get()
        System.out.println("upload successful")
      }
      client.submit(new TrySuccessFailureTest())
    }
    }

  } catch {
    case e: Exception =>
      e.printStackTrace()
  } finally {
    client.stop(true)
  }
}
