import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by Frank on 2017/11/10.
 */
public class WordCount {
    //!!!!!!一定注意在聚合操作之前要将聚合的KV类型提前注册如下
    static SparkConf conf = new SparkConf().setAppName("app")
            .registerKryoClasses(new Class<?>[]{
                    org.apache.hadoop.io.IntWritable.class,
                    org.apache.hadoop.io.Text.class
            });
    static JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] a){

        sc.textFile("hdfs://192.168.4.106:9000/README.md")

                //一行一个元素--->一个单词一个元素
                .flatMap(new FlatMapFunction<String,String>() {
                    @Override
                    public Iterator<String> call(String o) throws Exception {
                        return Arrays.asList(o.split(" ")).iterator();
                    }
                })

                //一个单词一个元素--->[单词,1]为元素
                .mapToPair(new PairFunction<String, Text, IntWritable>() {
                    @Override
                    public Tuple2<Text, IntWritable> call(String s) throws Exception {
                        return new Tuple2<>(new Text(s),new IntWritable(1));
                    }
                })

                //对相同的单词 的个数进行聚合(相加)
                .reduceByKey(new Function2<IntWritable, IntWritable, IntWritable>() {
                    @Override
                    public IntWritable call(IntWritable i, IntWritable i2) throws Exception {
                        return new IntWritable(i.get()+i2.get());
                    }
                })

                //结果保存到HDFS另一个文件下，以便日后使用
                .saveAsHadoopFile("hdfs://192.168.4.106:9000/res2",Text.class,
                        IntWritable.class,SequenceFileOutputFormat.class);
    }
}
