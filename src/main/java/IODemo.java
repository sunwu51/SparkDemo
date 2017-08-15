import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Frank on 2017/8/4.
 */
public class IODemo {
    static SparkConf conf = new SparkConf().setAppName("app").setMaster("local");
    static JavaSparkContext sc = new JavaSparkContext(conf);
    public static void main(String[] a){

        //准备好rdd和pairrdd
        JavaRDD textRdd = sc.textFile("hdfs://wy:9000/testpath/test.txt");

        JavaRDD words = textRdd.flatMap(new FlatMapFunction<String,String>() {
            public Iterator<String> call(String o) throws Exception {
                return Arrays.asList(o.split(" ")).iterator();
            }
        });
        final int[] num = {0};
        final AccumulatorV2 acc=sc.sc().longAccumulator();
        Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});
        broadcastVar.value();
        words.foreach(new VoidFunction<String>() {
            public void call(String o) throws Exception {
                num[0] ++;
                acc.add(1l);
            }
        });
        JavaRDD f=textRdd.coalesce(3);
        System.out.println(num[0]);
        System.out.println(acc.value());
        System.out.println(words.count());
    }
    public static void eachPrint(JavaPairRDD rdd){
        System.out.println("-------------------------------------------");
        rdd.foreach(new VoidFunction() {
            public void call(Object o) throws Exception {
                Tuple2 tuple2=(Tuple2)o;
                System.out.println(tuple2._1+":"+tuple2._2);
            }
        });
    }
    public static void eachPrint(JavaRDD rdd){
        System.out.println("-------------------------------------------");
        rdd.foreach(new VoidFunction() {
            public void call(Object s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
