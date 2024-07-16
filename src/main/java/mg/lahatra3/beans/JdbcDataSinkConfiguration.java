package mg.lahatra3.beans;

import java.util.Objects;

public class JdbcDataSinkConfiguration {

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final String dbtable;
    private final String numPartitions;
    private final String batchSize;


    /**
     * Constructs a new {@code JdbcDataSinkConfiguration} instance.
     * 
     * @param jdbcUrl jdbcUrl JDBC connection URL
     * @param user username Database username
     * @param password password Database password
     * @param dbtable dbtable Target database table
     * @param numPartitions numPartitions Number of partitions for data processing
     * @param batchSize batchSize Batch size for data processing
     */

    public JdbcDataSinkConfiguration(
        String jdbcUrl, 
        String user, 
        String password,
        String dbtable,
        String numPartitions,
        String batchSize
    ) {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl cannot be null ...");
        Objects.requireNonNull(user, "user cannot be null ...");
        Objects.requireNonNull(password, "password cannot be null ...");
        Objects.requireNonNull(dbtable, "dbtable cannot be null ...");
        Objects.requireNonNull(numPartitions, "numPartition cannot be null ...");
        Objects.requireNonNull(batchSize, "batchSize cannot be null ...");

        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.dbtable = dbtable;
        this.numPartitions = numPartitions;
        this.batchSize = batchSize;
    }

    // Getter methods
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDbtable() {
        return dbtable;
    }

    public String getNumPartitions() {
        return numPartitions;
    }

    public String getBatchSize() {
        return batchSize;
    }
}
