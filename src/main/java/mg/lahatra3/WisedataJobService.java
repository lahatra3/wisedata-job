package mg.lahatra3;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import mg.lahatra3.beans.CsvDataSourceConfiguration;
import mg.lahatra3.beans.JdbcDataSinkConfiguration;
import mg.lahatra3.beans.SparkConfiguration;
import mg.lahatra3.reader.CsvDataReader;
import mg.lahatra3.writer.JdbcDataWriter;

public class WisedataJobService {

    public WisedataJobService() {}

    public void process() {
        
        CsvDataSourceConfiguration csvDataSourceConfiguration = new CsvDataSourceConfiguration(
            "/home/jhs/Projets/WISEDATA/wisedata-job/local/data.csv"
        );

        JdbcDataSinkConfiguration jdbcDataSinkConfiguration = new JdbcDataSinkConfiguration(
            "jdbc:postgresql://localhost:5431/wisedata", 
            "", 
            "",
            "wisedata_db_test",
            131,
            131
        );

        SparkConfiguration sparkConfiguration = new SparkConfiguration(
            "Wisedata",
            "local[*]",
            "3",
            "4",
            "3g",
            "-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
        );

        SparkConf sparkConf = new SparkConf()
            .setAppName(sparkConfiguration.getAppName())
            .setMaster(sparkConfiguration.getMasterUrl())
            .set("spark.executor.instances", sparkConfiguration.getExecutor())
            .set("spark.executor.memory", sparkConfiguration.getMemory())
            .set("spark.driver.memory", sparkConfiguration.getMemory())
            .set("spark.executor.cores", sparkConfiguration.getCore())
            .set("spark.executor.extraJavaOptions", sparkConfiguration.getExtraJavaOptions())
            .set("spark.driver.extraJavaOptions", sparkConfiguration.getExtraJavaOptions());
        
        SparkSession sparkSession = SparkSession.builder()
            .config(sparkConf).getOrCreate();

        System.out.println("Start reading data ...");
        CsvDataReader csvDataReader = new CsvDataReader(sparkSession, csvDataSourceConfiguration);
        Dataset<Row> dataset = csvDataReader.get();

        System.out.println("Start writing data ...");
        JdbcDataWriter jdbcDataWriter = new JdbcDataWriter(jdbcDataSinkConfiguration);
        jdbcDataWriter.accept(dataset);
    }
}
