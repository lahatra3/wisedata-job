package mg.lahatra3.beans;

import java.util.Objects;

public class CsvDataSourceConfiguration {

    private final String absolutePath;
    // private final int fetchSize;

    /**
     * Constructs a new {@code CsvDataSourceConfiguration} instance.
     * 
     * @param absolutePath absolutePath Absolute path of file
     * 
     */

    public CsvDataSourceConfiguration(
        String absolutePath
    ) {
        Objects.requireNonNull(absolutePath, "absolutePath cannot be null ...");
    
        this.absolutePath = absolutePath;
    }

    // Getter methods
    public String getAbsolutePath() {
        return absolutePath;
    }
}
