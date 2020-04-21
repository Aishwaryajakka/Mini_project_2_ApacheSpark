package com.spark.infsci2750.part2.config;

public class ApplicationConstants {
    public static final String APPLICATION_NAME = "part2-assignments";
    public static final String USER_ARTISTS_SCHEMA = "userID\tartistID\tweight";
    public static final String USER_ARTISTS_SCHEMA_DELIMITTER = "\t";
    public static final boolean USER_ARTISTS_SCHEMA_HEADER_PRESENT = Boolean.TRUE;

    public static final String LISTENING_COUNTS_TEMPVIEW = "userartists";
    public static final String LISTENING_COUNTS_SQL = "select artistId, sum(weight) as listeningCount from userartists group by artistId order by listeningCount desc";
}
