package geekbang.spark.sql.extension

import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSessionExtensions

class MySparkSessionExtension extends (SparkSessionExtensions => Unit){
  lazy val logger = LoggerFactory.getLogger("MySparkSessionExtension")

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session => {
      logger.warn("My spark session extension is started")
      new MyPushDown(session)
      }
    }
  }
}
