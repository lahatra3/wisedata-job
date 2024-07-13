package mg.lahatra3.writer;

import java.util.function.Consumer;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import mg.lahatra3.beans.JdbcDataSinkConfiguration;

public class JdbcDataWriter implements Consumer<Dataset<Row>> {

    private final JdbcDataSinkConfiguration jdbcDataSinkConfiguration;

    public JdbcDataWriter(
        JdbcDataSinkConfiguration jdbcDataSinkConfiguration
    ) {
        this.jdbcDataSinkConfiguration = jdbcDataSinkConfiguration;
    }

    @Override
    public void accept(Dataset<Row> dataset) {
        DataFrameWriter<Row> dataFrameWriter = dataset.write();
        dataFrameWriter.format("jdbc")
            .mode("append")
            .option("url",  jdbcDataSinkConfiguration.getJdbcUrl())
            .option("user", jdbcDataSinkConfiguration.getUser())
            .option("password", jdbcDataSinkConfiguration.getPassword())
            .option("dbtable", jdbcDataSinkConfiguration.getDbtable())
            .option("batchsize", jdbcDataSinkConfiguration.getBatchSize())
            .save();
    }
}
