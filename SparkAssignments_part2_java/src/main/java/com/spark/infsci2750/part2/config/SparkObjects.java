package com.spark.infsci2750.part2.config;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import static com.spark.infsci2750.part2.config.ApplicationConstants.APPLICATION_NAME;
import static com.spark.infsci2750.part2.config.SparkObjectConstants.ACTIVE_APPLICATION_MASTER;
import static com.spark.infsci2750.part2.config.SparkObjectConstants.SPARK_LOG_LEVEL;

public class SparkObjects {

    private static SparkSession sparkInstance = null;

    public static SparkConf getSparkConfiguration() {
        return new SparkConf()
                .setAppName(APPLICATION_NAME)
                .setMaster(ACTIVE_APPLICATION_MASTER);
    }

    public static SparkSession getSparkSession() {
        if (sparkInstance == null) {
            sparkInstance = SparkSession.builder()
                    .config(getSparkConfiguration())
                    .config("java.net.useSystemProxies", "true")
                    .config("fs.file.impl", LocalFileSystem.class.getName())
                    .config("fs.hdfs.impl", DistributedFileSystem.class.getName())
                    .getOrCreate();
        }
        sparkInstance.sparkContext().setLogLevel(SPARK_LOG_LEVEL);
        return sparkInstance;
    }
}
