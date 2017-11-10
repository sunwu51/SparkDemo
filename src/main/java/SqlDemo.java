import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.catalyst.expressions.aggregate.Max;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;

import java.io.File;

import static org.apache.spark.sql.functions.*;


/**
 * Created by Frank on 2017/8/6.
 */
public class SqlDemo {

    public static void main(String[] a){

        //创建自己的仓库存储目录
//        String warehouseLocation = new File("/spark-hive-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Hive")
//                .config("spark.sql.warehouse.dir", warehouseLocation)
//                .enableHiveSupport()
                .master("local")
                .getOrCreate();

        //enableHiveSupport
        //可以直接运行hql
//        spark.sql("CREATE TABLE test(id int,name string,age int)ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'");
//        spark.sql("load data local inpath '/root/2.txt' into table test");
//        spark.sql("select * from test").show();
//
        Dataset<Row> df = spark.read().json("1.json");

        df.show();
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people where name like '%Fra%'");

        sqlDF.show();

    }
}
