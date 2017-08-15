import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.DoubleAccumulator;


import java.util.Arrays;

/**
 * Created by Frank on 2017/8/4.
 */
public class ValDemo {
    static SparkConf conf = new SparkConf().setAppName("app").setMaster("local");
    static JavaSparkContext sc = new JavaSparkContext(conf);
    public static void main(String[] a){
//        AccumulatorV2 accumulatorV2 =new DoubleAccumulator();
//        final Accumulator<Integer> accumulator1 = sc.accumulator(0,"SingleCount");
        final AccumulatorV2 accumulator1 = sc.sc().longAccumulator();
        JavaRDD rdd = sc.parallelize(Arrays.asList(0,1,2,3,4,5,6,7,8,9));
        JavaRDD newrdd = rdd.map(new Function<Integer,Integer>() {
            public Integer call(Integer o) throws Exception {
                if(o%2==0){
                    accumulator1.add(new Long(1));;
                    return 0;
                }
                return 1;
            }
        });
        //直接打印  因为newrdd惰性的，还没用到所以还没求说以此时计数为0
        System.out.println(accumulator1.value());
        //action操作 会执行并计数，此时计数为5
        newrdd.count();
        System.out.println(accumulator1.value());
        //再来一个action 因为newrdd在上一次action时并没有持久化，所以此时还会再执行一次计数10
        newrdd.count();
        System.out.println(accumulator1.value());
        //两个action，cache+count。cache会切断前后关联，count执行在cache后的rdd上，因而是15
        newrdd.cache().count();
        System.out.println(accumulator1.value());


    }
}
