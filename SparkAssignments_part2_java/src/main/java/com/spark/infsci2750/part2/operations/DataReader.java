package com.spark.infsci2750.part2.operations;

import com.spark.infsci2750.part2.model.SchemaModel;
import com.spark.infsci2750.part2.util.SchemaUtility;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

import static com.spark.infsci2750.part2.config.SparkObjectConstants.MIN_PARTITIONS;

public class DataReader implements Serializable {

    public JavaRDD<String> readDataFromFile(String inputPath, SparkSession sparkSession, boolean filterHeader) {
        if (filterHeader) {
            JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile(inputPath, MIN_PARTITIONS).toJavaRDD();
            String header = javaRDD.first();
            JavaRDD<String> javaRDDToReturn = javaRDD.filter(line -> !line.equalsIgnoreCase(header));
            return javaRDDToReturn;
        } else {
            return sparkSession.sparkContext().textFile(inputPath, MIN_PARTITIONS).toJavaRDD();
        }
    }

    public Dataset getDataSetByRDD(String inputPath, SparkSession sparkSession, boolean filterHeader, SchemaModel schemaModel) {
        JavaRDD<String> javaRDD = readDataFromFile(inputPath, sparkSession, filterHeader);
        JavaRDD<Row> rowRDD = javaRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String record) throws Exception {
                String[] attributes = record.split(schemaModel.getSchemaDelimiter());
                return RowFactory.create(attributes[0].trim(), attributes[1].trim(), attributes[2].trim());
            }
        });
        StructType structType = SchemaUtility.getStructTypeForSchema(schemaModel.getSchemaString(), schemaModel.getSchemaDelimiter());
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowRDD, structType);
        return dataFrame;
    }

    public Dataset getDataSetBySparkSql(String inputPath, SparkSession sparkSession, boolean filterHeader, SchemaModel schemaModel) {
        StructType structType = SchemaUtility.getStructTypeForSchema(schemaModel.getSchemaString(), schemaModel.getSchemaDelimiter());
        Dataset<Row> dataFrame = sparkSession
                .read()
                .format("csv")
                .schema(structType)
                .option("delimiter", schemaModel.getSchemaDelimiter())
                .option("header", filterHeader)
                .load(inputPath);
        return dataFrame;
    }
}
