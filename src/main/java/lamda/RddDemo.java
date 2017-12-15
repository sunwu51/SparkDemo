package lamda;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Frank on 2017/8/2.
 */
public class RddDemo{
    static SparkConf conf = new SparkConf().setAppName("app").setMaster("local");
    static JavaSparkContext sc = new JavaSparkContext(conf);
    public static void main(String[] a){
        List<String> list= Arrays.asList("hello frank","hello david","hi lidazhao","hi lidazhao");
        JavaRDD<String> rdd = sc.parallelize(list);
        //单个rdd处理
        eachPrint(filter(rdd));
        eachPrint(map(rdd));
        eachPrint(distinct(rdd));
        eachPrint(flatmap(rdd));
        eachPrint(sample(rdd));
        //处理两个rdd
        eachPrint(union(filter(rdd),map(rdd)));
        eachPrint(intersection(filter(rdd),rdd));
        eachPrint(subtract(rdd,filter(rdd)));
        eachPrint(cartesian(rdd,filter(rdd)));
        //行动操作 返回实际类型而不是rdd
        print(rdd.collect());//list
        print(rdd.count());//long
        print(rdd.countByValue());//map
        print(rdd.top(3));//list
        print(rdd.take(3));
        print(rdd.takeOrdered(3));
        print(rdd.takeSample(false,1));
        print(reduce(rdd));
        print(fold(rdd));
        print(aggregate2(rdd));
        //JavaDoubleRDD相当于JavaRDD<Double>，但是又专门封装了数学方法如max sum mean期望 stdev标准差 variance方差
        print(aggregate3(sc.parallelize(Arrays.asList(1,2,3,4,5)).mapToPair(new PairFunction<Integer,Integer,Integer>() {
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer,1);
            }
        })));

        print(aggregate4(sc.parallelize(Arrays.asList(1,2,3,4,5))));
    }
    //filter 过滤含有hello的
    public static JavaRDD filter(JavaRDD rdd){
        return rdd.filter((Object s) -> ((String)s).contains("hello"));
    }
    //map 对每一个String加[[]]的处理
    public static JavaRDD map(JavaRDD rdd){
        return rdd.map((Object  s) -> "[["+s+"]]");
    }
    //distinct 去重复
    public static JavaRDD distinct(JavaRDD rdd){
        return rdd.distinct();
    }
    //flatmap 一对多返回
    public static JavaRDD flatmap(JavaRDD rdd){
        return rdd.flatMap((Object s) -> Arrays.asList(((String)s).split(" ")).iterator());
    }
    //sample 随机抽样设定概率
    public static JavaRDD sample(JavaRDD rdd){
        return rdd.sample(false,0.3);
    }
    //union 合并（不去重复）
    public static JavaRDD union(JavaRDD rdd1,JavaRDD rdd2){
        return rdd1.union(rdd2);
    }
    //intersection 交集
    public static JavaRDD intersection(JavaRDD rdd1,JavaRDD rdd2){
        return rdd1.intersection(rdd2);
    }
    //subtract 从rdd1中移除rdd2的
    public static JavaRDD subtract(JavaRDD rdd1,JavaRDD rdd2){
        return rdd1.subtract(rdd2);
    }
    //cartesian 笛卡尔积
    public static JavaPairRDD cartesian(JavaRDD rdd1, JavaRDD rdd2){
        return rdd1.cartesian(rdd2);
    }
    //reduce a+b+c+e+d...
    public static String reduce(JavaRDD rdd){
        return rdd.reduce((Object o, Object o2) -> o+"+"+o2).toString();
    }
    //fold 类似于reduce不过要提供初值先进行一次运算
    public static String fold(JavaRDD rdd){
        return rdd.fold("chushi", (Object o, Object o2) -> o+"+"+o2).toString();
    }
    //aggregate 也类似于reduce但是运算返回值可以不同于输入值
    public static Object aggregate(JavaRDD rdd){
        return rdd.aggregate(new kv("",0), (Object o, Object o2) ->{
                    ((kv)o).count++;
                    ((kv)o).word+=o2;
                    return o;
            }
        , (Object o, Object o2) -> {
                    ((kv)o).word+=((kv)o2).word;
                    ((kv)o).count+=((kv)o2).count;
                    return o;
        });
    }
    public static Object aggregate2(JavaRDD rdd){
        return rdd.aggregate(0, (Object o, Object o2) -> {
                return ((Integer)o) + ((String)o2).length();
            }
            , (Object o, Object o2) -> {
                    return ((Integer)o)+((Integer)o2);
            });
    }
    public static Object aggregate3(JavaPairRDD rdd){
        return rdd.aggregate(
                new Tuple2<Integer,Integer>(0,0),
               (Object o, Object o2) -> new Tuple2<Integer, Integer>(((Tuple2<Integer,Integer>)o)._1+((Tuple2<Integer,Integer>)o2)._1,((Tuple2<Integer,Integer>)o)._2+((Tuple2<Integer,Integer>)o2)._2),
               (Object o, Object o2) -> new Tuple2<Integer, Integer>(((Tuple2<Integer,Integer>)o)._1+((Tuple2<Integer,Integer>)o2)._1,((Tuple2<Integer,Integer>)o)._2+((Tuple2<Integer,Integer>)o2)._2)
        );
    }
    public static Object aggregate4(JavaRDD rdd){
        return rdd.aggregate(new Tuple2<>(0, 0),
                (Object o, Object o2) ->new Tuple2<>(((Tuple2<Integer, Integer>)o)._1() + (Integer) o2, ((Tuple2<Integer, Integer>)o)._2() + 1),
                (Object o, Object o2) ->new Tuple2<Integer, Integer>(((Tuple2<Integer, Integer>)o)._1+((Tuple2<Integer, Integer>)o2)._1,((Tuple2<Integer, Integer>)o)._2+((Tuple2<Integer, Integer>)o2)._2)
        );
    }
    public static class kv implements Serializable {
        String word;
        Integer count;
        kv(String word,Integer count){
            this.word=word;
            this.count=count;
        }
        public String toString(){
            return word+"\r\n"+count;
        }
    }
    public static void eachPrint(JavaRDD rdd){
        System.out.println("-------------------------------------------");
        rdd.foreach((Object s) -> {
            System.out.println(s);
        });
    }
    public static void eachPrint(JavaPairRDD<String,String> rdd){
        System.out.println("-------------------------------------------");
        rdd.foreach((Tuple2<String, String> stringStringTuple2) -> {
                System.out.println(stringStringTuple2._1+":"+stringStringTuple2._2);
        });
    }
    public static void print(Object object){
        System.out.println("-------------------------------------------");
        System.out.println(object);
    }
}
