package lamda

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.PairFunction
import scala.Tuple2
import java.io.Serializable
import java.util.*

/**
 * Created by Frank on 2017/12/17.
 */
fun main(a: Array<String>){
    RddDemokt().run()
}
class RddDemokt {
    internal var conf = SparkConf().setAppName("app").setMaster("local")
    internal var sc = JavaSparkContext(conf)
    fun run() {
        val list = Arrays.asList("hello frank", "hello david", "hi lidazhao", "hi lidazhao")
        val rdd = sc.parallelize(list)
        //单个rdd处理
        eachPrint(filter(rdd))
        eachPrint(map(rdd))
        eachPrint(distinct(rdd))
        eachPrint(flatmap(rdd))
        eachPrint(sample(rdd))
        //处理两个rdd
        eachPrint(union(filter(rdd), map(rdd)))
        eachPrint(intersection(filter(rdd), rdd))
        eachPrint(subtract(rdd, filter(rdd)))
        eachPrint(cartesian(rdd, filter(rdd)))
        //行动操作 返回实际类型而不是rdd
        print(rdd.collect())//list
        print(rdd.count())//long
        print(rdd.countByValue())//map
        print(rdd.top(3))//list
        print(rdd.take(3))
        print(rdd.takeOrdered(3))
        print(rdd.takeSample(false, 1))
        print(reduce(rdd))
        print(fold(rdd))
        print(aggregate2(rdd))
        //JavaDoubleRDD相当于JavaRDD<Double>，但是又专门封装了数学方法如max sum mean期望 stdev标准差 variance方差
        print(aggregate3(sc.parallelize(Arrays.asList(1, 2, 3, 4, 5)).mapToPair { integer -> Tuple2(integer, 1) }))

        print(aggregate4(sc.parallelize(Arrays.asList(1, 2, 3, 4, 5))))
    }

    //filter 过滤含有hello的
    fun filter(rdd: JavaRDD<String>): JavaRDD<String> {
        return rdd.filter { s: Any -> (s as String).contains("hello") }
    }

    //map 对每一个String加[[]]的处理
    fun map(rdd: JavaRDD<String>): JavaRDD<String> {
        return rdd.map { s: Any -> "[[$s]]" }
    }

    //distinct 去重复
    fun distinct(rdd: JavaRDD<String>): JavaRDD<String> {
        return rdd.distinct()
    }

    //flatmap 一对多返回
    fun flatmap(rdd: JavaRDD<String>): JavaRDD<String> {
        return rdd.flatMap { s: Any -> Arrays.asList(*(s as String).split(" ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()).iterator() }
    }

    //sample 随机抽样设定概率
    fun sample(rdd: JavaRDD<String>): JavaRDD<String> {
        return rdd.sample(false, 0.3)
    }

    //union 合并（不去重复）
    fun union(rdd1: JavaRDD<String>, rdd2: JavaRDD<String>): JavaRDD<String> {
        return rdd1.union(rdd2)
    }

    //intersection 交集
    fun intersection(rdd1: JavaRDD<String>, rdd2: JavaRDD<String>): JavaRDD<String> {
        return rdd1.intersection(rdd2)
    }

    //subtract 从rdd1中移除rdd2的
    fun subtract(rdd1: JavaRDD<String>, rdd2: JavaRDD<String>): JavaRDD<String> {
        return rdd1.subtract(rdd2)
    }

    //cartesian 笛卡尔积
    fun cartesian(rdd1: JavaRDD<String>, rdd2: JavaRDD<String>): JavaPairRDD<String, String> {
        return rdd1.cartesian(rdd2)
    }

    //reduce a+b+c+e+d...
    fun reduce(rdd: JavaRDD<String>): String {
        return rdd.reduce { o: Any, o2: Any -> o.toString() + "+" + o2 }.toString()
    }

    //fold 类似于reduce不过要提供初值先进行一次运算
    fun fold(rdd: JavaRDD<String>): String {
        return rdd.fold("chushi", { o: Any, o2: Any -> o.toString() + "+" + o2 }).toString()
    }

    //aggregate 也类似于reduce但是运算返回值可以不同于输入值
    fun aggregate(rdd: JavaRDD<kv>): Any {
        return rdd.aggregate(kv("", 0), { o: kv, o2: kv ->
            (o as kv).count++
            o.word += o2
            o
        }) { o: kv, o2: kv ->
            (o as kv).word += (o2 as kv).word
            (o as kv).count += o2.count
            o
        }
    }

    fun aggregate2(rdd: JavaRDD<String>): Any {
        return rdd.aggregate(0, { o: Any, o2: Any -> o as Int + (o2 as String).length }) { o: Any, o2: Any -> o as Int + o2 as Int }
    }

    fun aggregate3(rdd: JavaPairRDD<Int, Int>): Any {
        return rdd.aggregate(
                Tuple2(0, 0),
                { o: Tuple2<Int,Int>, o2: Tuple2<Int,Int> -> Tuple2(o._1 + o2._1, o._2 + o2._2) }
        ) { o: Tuple2<Int,Int>, o2: Tuple2<Int,Int> -> Tuple2(o._1 + o2._1, o._2 + o2._2) }
    }

    fun aggregate4(rdd: JavaRDD<Int>): Any {
        return rdd.aggregate(Tuple2(0, 0),
                { o: Tuple2<Int, Int>, o2: Int -> Tuple2(o._1() + o2, o._2() + 1) }
        ) { o: Tuple2<Int, Int>, o2: Tuple2<Int, Int> -> Tuple2(o._1 + o2._1, o._2 + o2._2) }
    }

    class kv internal constructor(internal var word: String, internal var count: Int) : Serializable {
        override fun toString(): String {
            return word + "\r\n" + count
        }
    }

    fun eachPrint(rdd: JavaRDD<String>) {
        println("-------------------------------------------")
        rdd.foreach { s: Any -> println(s) }
    }

    fun eachPrint(rdd: JavaPairRDD<String, String>) {
        println("-------------------------------------------")
        rdd.foreach { stringStringTuple2: Tuple2<String, String> -> println(stringStringTuple2._1 + ":" + stringStringTuple2._2) }
    }

    fun print(`object`: Any) {
        println("-------------------------------------------")
        println(`object`)
    }
}