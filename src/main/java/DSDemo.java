import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;


/**
 * Created by Frank on 2017/8/6.
 */
public class DSDemo {

    public static void main(String[] a){
        SparkSession spark = SparkSession
                .builder()
                .appName("DS")
                .master("local")
                .getOrCreate();
        //先从文件获取ds<row>
        Dataset<Row> df = spark.read().json("1.json");

        //ds直接转JavaRDD<Row>
        JavaRDD<Row> rdd = df.javaRDD();

        //JavaRDD<Row>转JavaRDD<Person>
        JavaRDD<Person> rdd2 = rdd
                .map(new Function<Row, Person>() {
            @Override
            public Person call(Row row) throws Exception {
                return new Person(row.getLong(0),
                        row.getLong(1),
                        row.getString(2));
            }
        });
        rdd2.filter(new Function<Person, Boolean>() {
            @Override
            public Boolean call(Person person) throws Exception {
                return person.getAge()>22 && person.getAge()<36 && person.getName().contains("John");
            }
        });

        //JavaRDD<Person>转DS<Person>或DS<Row>
        Dataset<Person> ds = spark.createDataset(rdd2.rdd(),Encoders.javaSerialization(Person.class));
        Dataset<Row> dff = spark.createDataFrame(rdd2,Person.class);
        
        dff.show();
        System.out.println(ds.first().name);
//        System.out.println(dff.first().get(2));

    }
    //必须有getter setter否则转为df是空
    //必须实现Serializable否则无法转ds
    public static class Person implements Serializable{

        public long group;
        public long age;
        public String name;

        public long getAge() {
            return age;
        }

        public void setAge(long age) {
            this.age = age;
        }

        public long getGroup() {
            return group;
        }

        public void setGroup(long group) {
            this.group = group;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Person(long age, long group, String name){this.group=group;this.name=name;this.age=age;}
    }
}
