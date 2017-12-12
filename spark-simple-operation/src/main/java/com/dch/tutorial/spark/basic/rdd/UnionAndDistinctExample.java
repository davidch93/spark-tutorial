package com.dch.tutorial.spark.basic.rdd;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dch.tutorial.spark.config.SparkConfig;

/**
 * Spark with Union.
 * 
 * @author David.Christianto
 */
public class UnionAndDistinctExample {

	/**
	 * Union value with {@link JavaRDD}.
	 * <p>
	 * Return the union of this RDD and another one.
	 * </p>
	 */
	public void union(JavaRDD<Integer> numbers1RDD, JavaRDD<Integer> numbers2RDD) {
		JavaRDD<Integer> unionRDD = numbers1RDD.union(numbers2RDD);
		System.out.println(unionRDD.collect().toString());
	}

	/**
	 * Distinct value with {@link JavaRDD}.
	 * <p>
	 * Return a new RDD containing the distinct elements in this RDD.
	 * </p>
	 */
	public void distinct(JavaRDD<Integer> numbers1RDD) {
		JavaRDD<Integer> distinctRDD = numbers1RDD.distinct();
		System.out.println(distinctRDD.collect().toString());
	}

	public static void main(String... args) {
		JavaSparkContext context = SparkConfig.createSparkContext("local", "UnionAndDistinctExample");

		JavaRDD<Integer> numbers1RDD = context.parallelize(Arrays.asList(0, 5, 3, 0));
		JavaRDD<Integer> numbers2RDD = context.parallelize(Arrays.asList(8, 9, 3));

		UnionAndDistinctExample unionAndDistinctExample = new UnionAndDistinctExample();
		unionAndDistinctExample.union(numbers1RDD, numbers2RDD);
		unionAndDistinctExample.distinct(numbers1RDD);

		context.close();
	}
}
