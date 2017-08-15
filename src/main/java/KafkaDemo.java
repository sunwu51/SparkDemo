import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Frank on 2017/8/15.
 */
public class KafkaDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("kafka");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        Map<String,Integer> topics=new HashMap<>();
        topics.put("test",1);
        JavaPairDStream<String,String> input =
                KafkaUtils.createStream(jssc,"wb:2181,wy:2181","group1",topics);
//        input.print();
        input.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2.equals("frank");
            }
        }).print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();
    }
}
