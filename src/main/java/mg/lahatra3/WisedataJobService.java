package mg.lahatra3;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.github.cdimascio.dotenv.Dotenv;
import mg.lahatra3.beans.CsvDataSourceConfiguration;
import mg.lahatra3.beans.JdbcDataSinkConfiguration;
import mg.lahatra3.beans.SparkConfiguration;
import mg.lahatra3.reader.CsvDataReader;
import mg.lahatra3.writer.JdbcDataWriter;

public class WisedataJobService {

    public WisedataJobService() {}

    public void process() {

        Dotenv dotenv = Dotenv.load();
        

        String absolutePath = dotenv.get("CSV_DATASOURCE");

        CsvDataSourceConfiguration csvDataSourceConfiguration = new CsvDataSourceConfiguration(absolutePath);

        String jdbcUrl = dotenv.get("DB_JDBC_URL_DATASINK");
        String user = dotenv.get("DB_USER_DATASINK");
        String password = dotenv.get("DB_PASSWORD_DATASINK");
        String dbtable = dotenv.get("DB_TABLE_DATASINK");
        String numPartitions = dotenv.get("NUM_PARTITION_DATASINK");
        String batchSize = dotenv.get("BATCH_SIZE_DATASINK");

        JdbcDataSinkConfiguration jdbcDataSinkConfiguration = new JdbcDataSinkConfiguration(
            jdbcUrl, 
            user, 
            password,
            dbtable,
            numPartitions,
            batchSize
        );


        String appName = dotenv.get("SPARK_APPNAME");
        String masterUrl = dotenv.get("SPARK_MASTER_URL");
        String exector = dotenv.get("SPARK_EXECUTORS_NUMBER");
        String core = dotenv.get("SPARK_CORE_EXECUTOR");
        String memory = dotenv.get("SPARK_MEMORY_EXECUTOR");
        String extraJavaOptions = dotenv.get("SPARK_EXTRA_JAVA_OPTIONS");

        SparkConfiguration sparkConfiguration = new SparkConfiguration(
            appName,
            masterUrl,
            exector,
            core,
            memory,
            extraJavaOptions
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
