import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Frank on 2017/8/6.
 */
public class SqlDemo {
    static SparkConf conf = new SparkConf().setAppName("app").setMaster("local");
    static JavaSparkContext sc = new JavaSparkContext(conf);
    public static void main(String[] a){
        HiveContext hc=new HiveContext(sc);
        Dataset ds = hc.jsonFile("hdfs://wy:9000/testpath/test.txt/1.json");
        ds.registerTempTable("user");
        JavaRDD rdd=hc.sql("select name from user").toJavaRDD();
        JavaRDD<String> rddname = rdd.map(new Function<Row,String>() {
            public String call(Row o) throws Exception {
                return o.getString(0);
            }
        });

        System.out.println(rddname.collect());
    }
}
