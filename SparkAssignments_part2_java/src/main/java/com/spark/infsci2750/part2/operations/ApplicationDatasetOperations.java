package com.spark.infsci2750.part2.operations;

import org.apache.spark.sql.*;

import static com.spark.infsci2750.part2.config.ApplicationConstants.LISTENING_COUNTS_SQL;
import static com.spark.infsci2750.part2.config.ApplicationConstants.LISTENING_COUNTS_TEMPVIEW;

public class ApplicationDatasetOperations {

    public static Dataset getTotalListeningCountsByArtistsBySQL(Dataset<Row> dataSet, SparkSession sparkSession) {
        dataSet.registerTempTable(LISTENING_COUNTS_TEMPVIEW);
        Dataset<Row> listeningCounts = sparkSession.sql(LISTENING_COUNTS_SQL);
        return listeningCounts;
    }
}
