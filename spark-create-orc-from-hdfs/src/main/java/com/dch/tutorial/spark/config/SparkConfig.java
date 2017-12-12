package com.dch.tutorial.spark.config;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Configuration to create {@link JavaSparkContext} and other objects required
 * by Spark.
 * 
 * @author David.Christianto
 */
public class SparkConfig {

	/**
	 * Method used to create and get spark session.
	 * 
	 * @param master
	 *            The master URL to connect to
	 * @param applicationName
	 *            Set a name for your application. Shown in the Spark web UI.
	 * @return {@link SparkSession}
	 */
	public static SparkSession getSession(String master, String applicationName) {
		// @formatter:off
		return SparkSession.builder()
				.master(master)
				.appName(applicationName)
				.enableHiveSupport()
			.getOrCreate();
		// @formatter:on
	}
}
