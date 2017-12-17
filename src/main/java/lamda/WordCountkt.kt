package lamda

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2
import java.util.*

/**
 * Created by Frank on 2017/12/18.
 */
fun main(a: Array<String>){
    WordCountkt().run()
}
class WordCountkt {
    //!!!!!!一定注意在聚合操作之前要将聚合的KV类型提前注册如下
    internal var conf = SparkConf().setAppName("app")
            .registerKryoClasses(arrayOf<Class<*>>(IntWritable::class.java, Text::class.java))
    internal var sc = JavaSparkContext(conf)


    fun run() {

        sc.textFile("hdfs://192.168.4.106:9000/README.md")

                //一行一个元素--->一个单词一个元素
                .flatMap { o: String -> Arrays.asList(*o.split(" ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()).iterator() }

                //一个单词一个元素--->[单词,1]为元素
                .mapToPair { s: String -> Tuple2(Text(s), IntWritable(1)) }

                //对相同的单词 的个数进行聚合(相加)
                .reduceByKey { i: IntWritable, i2: IntWritable -> IntWritable(i.get() + i2.get()) }

                //结果保存到HDFS另一个文件下，以便日后使用
                .saveAsHadoopFile("hdfs://192.168.4.106:9000/res2", Text::class.java,
                        IntWritable::class.java, SequenceFileOutputFormat::class.java)
    }
}