package mg.lahatra3.reader;

import java.util.function.Supplier;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import mg.lahatra3.beans.CsvDataSourceConfiguration;

public class CsvDataReader implements Supplier<Dataset<Row>> {

    private final SparkSession sparkSession;
    private final CsvDataSourceConfiguration csvDataSourceConfiguration;

    public CsvDataReader(
        SparkSession sparkSession,
        CsvDataSourceConfiguration csvDataSourceConfiguration
    ) {
        this.sparkSession = sparkSession;
        this.csvDataSourceConfiguration = csvDataSourceConfiguration;
    }

    @Override
    public Dataset<Row> get() {
        
        DataFrameReader dataFrameReader = sparkSession.read();
        return dataFrameReader
            .option("delimiter", ",")
            .csv(csvDataSourceConfiguration.getAbsolutePath());
    }
}
