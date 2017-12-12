package com.dch.tutorial.spark.basic.rdd;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dch.tutorial.spark.config.SparkConfig;

import scala.Tuple2;

/**
 * Spark with CoGroup.
 * 
 * @author David.Christianto
 */
public class CoGroupExample {

	/**
	 * CoGroup value with {@link JavaPairRDD}.
	 * <p>
	 * For each key k in `this` or `other`, return a resulting RDD that contains a
	 * tuple with the list of values for that key in `this` as well as `other`.
	 * </p>
	 */
	public void coGroup(JavaPairRDD<String, String> pairsRDD1, JavaPairRDD<String, String> pairsRDD2) {
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> joinRDD = pairsRDD1.cogroup(pairsRDD2);
		System.out.println(joinRDD.collect().toString());
	}

	public static void main(String... args) {
		JavaSparkContext context = SparkConfig.createSparkContext("local", "CoGroupExample");

		// @formatter:off
		JavaPairRDD<String,String> pairsRDD1 = JavaPairRDD.fromJavaRDD(context.parallelize(
			Arrays.asList(
				new Tuple2<String,String>("index.html", "1.2.3.4"),
				new Tuple2<String,String>("about.html", "3.4.5.6"),
				new Tuple2<String,String>("index.html", "1.3.3.1"))
			)
		);
		JavaPairRDD<String,String> pairsRDD2 = JavaPairRDD.fromJavaRDD(context.parallelize(
			Arrays.asList(
				new Tuple2<String,String>("index.html", "Home"),
				new Tuple2<String,String>("index.html", "Welcome"),
				new Tuple2<String,String>("about.html", "About"),
				new Tuple2<String,String>("about.html", "About2"))
			)
		);
		//@formatter:on

		System.out.println(pairsRDD1.collect().toString());
		System.out.println(pairsRDD2.collect().toString());

		CoGroupExample coGroupExample = new CoGroupExample();
		coGroupExample.coGroup(pairsRDD1, pairsRDD2);

		context.close();
	}
}
