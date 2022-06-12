package geekbang.spark.homework

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

object RevertedIndex {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Reverted Index").master("local[*]").getOrCreate
    val rdd = spark.sparkContext.wholeTextFiles("data/input")

    rdd.flatMap(x => {
      //先获取文件名，拆分字符
      var sub = ArrayBuffer.empty[(String, Int)]
      val fileName = x._1.substring(x._1.lastIndexOf("/")+1)
      x._2.split(" ").foreach(e => {
        sub.+=((e+"_"+fileName, 1))
      })
      sub.iterator
      //将{字符_文件名}作为key，统计单文件中字符同一个字符出现次数
    }).reduceByKey(_+_).map(x => {
      val words = x._1.split("_")
      (words(0), words(1)+"_"+x._2)
      //将{字符}作为key, 将文件名和出现次数拼接起来
    }).reduceByKey(_+","+_).map(x => {
      //reduceByKey之后，重新将文件名和出现次数map成Array
      val sub = ArrayBuffer.empty[(String, Int)]
      for (w <- x._2.split(",")) {
        sub.append((w.split("_")(0), Integer.parseInt(w.split("_")(1))))
      }
      (x._1, sub.sorted)
    }).sortByKey().collect().foreach(println)  //打印结果

    spark.stop()
  }
}
