package com.spark.infsci2750.part2.runner;

import com.spark.infsci2750.part2.config.SparkObjects;
import com.spark.infsci2750.part2.model.SchemaModel;
import com.spark.infsci2750.part2.operations.ApplicationDatasetOperations;
import com.spark.infsci2750.part2.operations.DataReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.spark.infsci2750.part2.config.ApplicationConstants.*;

public class SparkApplication {

    public static void runTheApplication(String inputPath){
        SparkSession sparkSession = SparkObjects.getSparkSession();
        SchemaModel schemaModel = new SchemaModel();
        schemaModel.setSchemaString(USER_ARTISTS_SCHEMA) ;
        schemaModel.setSchemaDelimiter(USER_ARTISTS_SCHEMA_DELIMITTER);

        Dataset<Row> dataSet = new DataReader().getDataSetBySparkSql(
                inputPath,
                sparkSession,
                USER_ARTISTS_SCHEMA_HEADER_PRESENT,
                schemaModel);

        Dataset<Row> listeningCounts = ApplicationDatasetOperations.getTotalListeningCountsByArtistsBySQL(dataSet,sparkSession);
        listeningCounts.show();
    }
}
