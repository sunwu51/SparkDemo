import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;


import java.io.Serializable;
import java.security.SecureRandom;
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
        return rdd.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("hello");
            }
        });
    }
    //map 对每一个String加[[]]的处理
    public static JavaRDD map(JavaRDD rdd){
        return rdd.map(new Function<String,String>() {
            public String call(String s) throws Exception {
                return "[["+s+"]]";
            }
        });
    }
    //distinct 去重复
    public static JavaRDD distinct(JavaRDD rdd){
        return rdd.distinct();
    }
    //flatmap 一对多返回
    public static JavaRDD flatmap(JavaRDD rdd){
        return rdd.flatMap(new FlatMapFunction<String,String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
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
        return rdd.reduce(new Function2<String,String,String>() {
            public String call(String o, String o2) throws Exception {
                return o+"+"+o2;
            }
        }).toString();
    }
    //fold 类似于reduce不过要提供初值先进行一次运算
    public static String fold(JavaRDD rdd){
        return rdd.fold("chushi", new Function2<String,String,String>() {
            public String call(String o, String o2) throws Exception {
                return o+"+"+o2;
            }
        }).toString();
    }
    //aggregate 也类似于reduce但是运算返回值可以不同于输入值
    public static Object aggregate(JavaRDD rdd){
        return rdd.aggregate(new kv("",0), new Function2<kv,String,kv>() {
            public kv call(kv o, String o2) throws Exception {
                o.count++;
                o.word+=o2;
                return o;
            }
        }, new Function2<kv,kv,kv>() {
            public kv call(kv o, kv o2) throws Exception {
                o.word+=o2.word;
                o.count+=o2.count;
                return o;
            }
        });
    }
    public static Object aggregate2(JavaRDD rdd){
        return rdd.aggregate(0, new Function2<Integer, String, Integer>() {
            public Integer call(Integer o, String o2) throws Exception {
                return o + o2.length();
            }
        }, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer o, Integer o2) throws Exception {
                return o+o2;
            }
        });
    }
    public static Object aggregate3(JavaPairRDD rdd){
        return rdd.aggregate(new Tuple2<Integer,Integer>(0,0),
                new Function2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {
            public Tuple2<Integer,Integer> call(Tuple2<Integer,Integer> o, Tuple2<Integer,Integer> o2) throws Exception {
                return new Tuple2<Integer, Integer>(o._1+o2._1,o._2+o2._2);
            }
        }, new Function2<Tuple2<Integer,Integer>,  Tuple2<Integer,Integer>,  Tuple2<Integer,Integer>>() {
            public  Tuple2<Integer,Integer> call( Tuple2<Integer,Integer> o,  Tuple2<Integer,Integer> o2) throws Exception {
                return new Tuple2<Integer, Integer>(o._1+o2._1,o._2+o2._2);
            }
        });
    }
    public static Object aggregate4(JavaRDD rdd){
        return rdd.aggregate(new Tuple2<Integer, Integer>(0, 0),
                new Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {

                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> o, Integer o2) throws Exception {
                        return new Tuple2<Integer, Integer>(o._1() + o2, o._2() + 1);
                    }
                },
                new Function2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>() {

                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> o, Tuple2<Integer, Integer> o2) throws Exception {
                        return new Tuple2<Integer, Integer>(o._1+o2._1,o._2+o2._2);

                    }
                }
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
        rdd.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
    public static void eachPrint(JavaPairRDD<String,String> rdd){
        System.out.println("-------------------------------------------");
        rdd.foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2._1+":"+stringStringTuple2._2);
            }
        });
    }
    public static void print(Object object){
        System.out.println("-------------------------------------------");
        System.out.println(object);
    }
}
