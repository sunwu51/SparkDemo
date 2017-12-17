package lamda

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2
import java.util.*

/**
 * Created by Frank on 2017/12/17.
 */
class IODemokt {
    internal var conf = SparkConf().setAppName("app").setMaster("local")
    internal var sc = JavaSparkContext(conf)
    fun main(a: Array<String>) {

        //准备好rdd和pairrdd
        val textRdd = sc.textFile("hdfs://wy:9000/testpath/test.txt")

        val words = textRdd.flatMap { o: String -> Arrays.asList<String>(*o.split(" ".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()).iterator() }
        val num = intArrayOf(0)
        val acc = sc.sc().longAccumulator()
        val broadcastVar = sc.broadcast(intArrayOf(1, 2, 3))
        broadcastVar.value()
        words.foreach { o: String ->
            num[0]++
            acc.add(1L)
        }
        val f = textRdd.coalesce(3)
        println(num[0])
        println(acc.value())
        println(words.count())
    }

    fun eachPrint(rdd: JavaPairRDD<*, *>) {
        println("-------------------------------------------")
        rdd.foreach { o: Any ->
            val tuple2 = o as Tuple2<*, *>
            println(tuple2._1.toString() + ":" + tuple2._2)
        }
    }

    fun eachPrint(rdd: JavaRDD<*>) {
        println("-------------------------------------------")
        rdd.foreach { s: Any -> println(s) }
    }
}