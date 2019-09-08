import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Main {


    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        SparkSession sparkSession = SparkSession.builder().appName("cassandratable").master("spark://192.168.100.48:7077").getOrCreate();

        System.out.println(System.currentTimeMillis() - start);

        Map<String, String> cassandraMap = new HashMap<String, String>();

        cassandraMap.put("spark.cassandra.connection.host", "192.168.100.50"); //localhost
        cassandraMap.put("spark.cassandra.connection.port", "9042"); //9042
        cassandraMap.put("keyspace", "test");
        cassandraMap.put("table", "my_table");

        Dataset<Row> cassandraTableData = sparkSession.read().format("org.apache.spark.sql.cassandra").options(cassandraMap).load();

        System.out.println(System.currentTimeMillis() - start);

        System.out.println("Column Names : " + Arrays.asList(cassandraTableData.columns()));
        System.out.println("Table Schema : " + cassandraTableData.schema());
    }

}
