package com.dch.tutorial.spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Configuration to create {@link JavaSparkContext} and other objects required
 * by Spark.
 * 
 * @author David.Christianto
 */
public class SparkConfig {

	/**
	 * Method used to create default configuration {@link SparkConf} by master and
	 * application name.
	 * 
	 * @param master
	 *            The master URL to connect to
	 * @param applicationName
	 *            Set a name for your application. Shown in the Spark web UI.
	 * @return {@link JavaSparkContext}
	 */
	public static JavaSparkContext createSparkContext(String master, String applicationName) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster(master);
		sparkConf.setAppName(applicationName);
		return new JavaSparkContext(sparkConf);
	}
}
