/**
 * Created by Frank on 2017/8/6.
 */
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


public class StreamDemo {
    public static void main(String[] a) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("spark://wb:7077").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost",9999);
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                }
                );
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        JavaPairDStream<String, Integer> windowedWordCounts = pairs.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer+integer2;
                    }
                },Durations.seconds(30), Durations.seconds(10)
                // (i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10)
        );
        windowedWordCounts.print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}

