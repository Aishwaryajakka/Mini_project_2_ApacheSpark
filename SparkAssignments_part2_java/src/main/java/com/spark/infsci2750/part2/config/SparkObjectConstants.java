package com.spark.infsci2750.part2.config;

public class SparkObjectConstants {
    public static final String LOCAL_APPLICATION_MASTER ="local[1]";
    public static final String YARN_APPLICATION_MASTER ="yarn";
    public static final String ACTIVE_APPLICATION_MASTER =LOCAL_APPLICATION_MASTER;
    public static final String SPARK_LOG_LEVEL = "ERROR";
    public static final int MIN_PARTITIONS = 1;
}
