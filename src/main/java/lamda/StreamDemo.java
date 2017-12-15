package lamda; /**
 * Created by Frank on 2017/8/6.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


public class StreamDemo {
    public static void main(String[] a) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines =
                jssc.socketTextStream(
                        "localhost",9999);
        JavaPairDStream<String, Integer> wc = lines
                .flatMap((String s) -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair((String s) -> new Tuple2<String, Integer>(s,1))
                .reduceByKey((Integer integer, Integer integer2) -> integer+integer2)
                .reduceByKeyAndWindow(
                        (Integer integer, Integer integer2)->integer+integer2,
                        Durations.seconds(10),
                        Durations.seconds(3)
                    // (i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10)
            );
        wc.print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}

