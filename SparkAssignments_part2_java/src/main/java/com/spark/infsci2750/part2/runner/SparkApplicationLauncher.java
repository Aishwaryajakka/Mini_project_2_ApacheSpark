package com.spark.infsci2750.part2.runner;

public class SparkApplicationLauncher {


    public static void main(String[] args) {
        args = new String[1];
        args[0] = "/Users/aishwaryajakka/Downloads/hetrec2011-lastfm-2k/user_artists.dat";

        String inputPath = args[0];
        SparkApplication.runTheApplication(inputPath);
    }
}
