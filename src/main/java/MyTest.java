import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by Frank on 2017/8/6.
 */
public class MyTest {
   //Wins Terabyte Sort Benchmark: One of Yahoo's Hadoop clusters sorted 1 terabyte of data in 209 seconds, which beat the previous record of 297 seconds in the annual general purpose (Daytona) terabyte sort benchmark. This is the first time that either a Java or an open source program has won.\n";
    public static void main(String[] args) throws IOException {
        byte[] buf=new byte[1024*1024];
        int len = new FileInputStream("C:\\Users\\Frank\\Downloads\\test (1).txt").read(buf);
        String a = new String(buf,0,len);
        System.out.println(a.split(" ").length);
    }

}
