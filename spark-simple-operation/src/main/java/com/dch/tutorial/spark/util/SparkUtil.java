package com.dch.tutorial.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Util class to create {@link JavaSparkContext} and other objects required by Spark.
 *
 * <p><b>*NOTE</b><br/>
 * This example project doesn't use SparkQL so I'm just use {@link SparkConf}.
 * If you want to use SparkQL, then you must add new dependency
 * spark-sql-${scala.version} and use SparkSession instead of.
 *
 * @author David.Christianto
 */
public class SparkUtil {

    /**
     * Method used to create default configuration {@link SparkConf} by master and
     * application name.
     *
     * @param master          The master URL to connect to
     * @param applicationName Set a name for your application. Shown in the Spark web UI.
     * @return {@link JavaSparkContext}
     */
    public static JavaSparkContext createSparkContext(String master, String applicationName) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(master);
        sparkConf.setAppName(applicationName);
        return new JavaSparkContext(sparkConf);
    }
}
