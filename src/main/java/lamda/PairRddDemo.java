package lamda;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Frank on 2017/8/3.
 */
//!!!!!注意javaPairRdd是javaRDD的子集，所以javaRdd的函数他都有
public class PairRddDemo {
    static SparkConf conf = new SparkConf().setAppName("app").setMaster("local");
    static JavaSparkContext sc = new JavaSparkContext(conf);
    public static void main(String[] a){
        List<Integer> list= Arrays.asList(1,2,3,3,2,1,6,6,3);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaPairRDD<Integer,Integer> pairRDD=rdd.mapToPair((Integer o) -> new Tuple2(o,1));

        JavaPairRDD<Integer,Integer> pairRDD2=rdd.mapToPair((Integer o) -> new Tuple2(o,2));
        //单个rdd转换操作
        eachPrint(groupBykey(pairRDD));
        eachPrint(reduceByKey(pairRDD));
        eachPrint(combineByKey(pairRDD));
        eachPrint(mapValues(pairRDD));
        eachPrint(flatMapValues(pairRDD));
        eachPrint(pairRDD.keys());
        eachPrint(pairRDD.values());
        eachPrint(pairRDD.sortByKey());
        //两个rdd装换操作
        //key交集的剔除
        eachPrint(pairRDD.subtractByKey(pairRDD2));
        //key一样的笛卡尔乘积还有righOuterJoin和leftOuterjoin
        eachPrint(pairRDD.join(pairRDD2));
        //lookUp取出某一个特定key的value集返回一个list
        eachPrint(sc.parallelize(pairRDD.lookup(new Integer(1))));
        //数据分区，良好的数据分区对join bykey等运算是非常又用的，可以直接在一台机子上完成运算
        //减少了网络传输
        //下面是根据hash分区最多分100个区的写法，注意分区完了要持久化，否则下次还是原样的分布
        pairRDD.partitionBy(new HashPartitioner(100)).persist(StorageLevel.MEMORY_AND_DISK());
        //对于输出结果有时候我们也想让他分好区，比如map操作想让返回的和原来的在一个分区
        //但是spark的这些方法都是不按照原来的rdd存储的，是随机的，只有pair的一些方法会和原来保持一致
        //cogroup groupWith join（多个） groupByKey reduceByKey combineByKey sort 【flat】mapValues(父rdd有分区) filter（父rdd有分区）
    }
    //groupByKey把同一个key的value放到一个list中，返回缩小版的pairRdd
    public static JavaPairRDD groupBykey(JavaPairRDD pairRDD){
        return pairRDD.groupByKey();
    }
    //reduceByKey把同一个key的value进行聚合成一个值，返回缩小版的pairRdd
    public static JavaPairRDD reduceByKey(JavaPairRDD<Integer,Integer> pairRDD){
        return pairRDD.reduceByKey((Integer o, Integer o2) -> o+o2);
    }
    //combineByKey把同一个key的value拿出来进行分析(求均值)
    public static JavaPairRDD combineByKey(JavaPairRDD<Integer,Integer> pairRDD){
        return pairRDD.combineByKey(
                //注意三个参数都是对相同的key的操作
                //第一个参数，如果这个key是初次出现则返回一个 value:1的初值tuple
                (Integer o) -> new Tuple2<Integer, Integer>(o, 1),

                //第二个参数如果不是第二次出现的key，则初值tuple的value字段+这个key的value，然后初值tuple的num字段+1
                //第三个以此类推
                (Tuple2<Integer, Integer> o, Integer o2) -> new Tuple2<Integer, Integer>(o._1() + o2, o._2() + 1),

                //最后初值tuple加完后（其实可以算均值了）但是因为第二个参数是在各个分区上计算完的结果，还需要最后的汇总
                //第三个参数就是汇总各个分区的value的和，num的和。这样最后返回的就是 key:[value的和，value的个数]
                (Tuple2<Integer,Integer> o, Tuple2<Integer,Integer> o2) -> new Tuple2<Integer, Integer>(o._1() + o2._1(),o._2()+o2._2())
        );
    }
    //mapValues只对value进行map运算key保持返回等大的pairRdd
    public static JavaPairRDD mapValues(JavaPairRDD<Integer,Integer> pairRDD){
        return pairRDD.mapValues((Integer o) -> o.toString()+"st");
    }
    //flatMapValues只对value进行的flatmap运算返回扩大版pairRdd
    public static JavaPairRDD flatMapValues(JavaPairRDD pairRDD){
        return pairRDD.flatMapValues((Object o) -> Arrays.asList(o.toString()+"st","多个"));
    }

    public static void eachPrint(JavaPairRDD rdd){
        System.out.println("-------------------------------------------");
        rdd.foreach((Object o) -> {
            Tuple2 tuple2=(Tuple2)o;
            System.out.println(tuple2._1+":"+tuple2._2);
        });
    }
    public static void eachPrint(JavaRDD rdd){
        System.out.println("-------------------------------------------");
        rdd.foreach((Object s) -> {
            System.out.println(s);
        });
    }


}
