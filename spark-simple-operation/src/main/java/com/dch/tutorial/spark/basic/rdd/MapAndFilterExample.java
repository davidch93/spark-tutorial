package com.dch.tutorial.spark.basic.rdd;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.dch.tutorial.spark.config.SparkConfig;

/**
 * Spark with Map, Filter and FlatMap.
 * 
 * @author David.Christianto
 */
public class MapAndFilterExample {

	/**
	 * Map value with {@link Function} n * n.
	 * <p>
	 * Return a new RDD by applying a function to all elements of this RDD.
	 * </p>
	 */
	public void map(JavaRDD<Integer> numbersRDD) {
		JavaRDD<Integer> squaresRDD = numbersRDD.map(n -> n * n);
		System.out.println(squaresRDD.collect().toString());
	}

	/**
	 * Filter value with {@link Function} n % 2 == 0.
	 * <p>
	 * Return a new RDD containing only the elements that satisfy a predicate.
	 * </p>
	 */
	public void filter(JavaRDD<Integer> numbersRDD) {
		JavaRDD<Integer> evenRDD = numbersRDD.filter(n -> n % 2 == 0);
		System.out.println(evenRDD.collect().toString());
	}

	/**
	 * Map value with {@link FlatMapFunction} n, n * 2, n * 3.
	 * <p>
	 * Return a new RDD by first applying a function to all elements of this RDD,
	 * and then flattening the results.
	 * </p>
	 */
	public void flatMap(JavaRDD<Integer> numbersRDD) {
		JavaRDD<Integer> multipliedRDD = numbersRDD.flatMap(n -> Arrays.asList(n, n * 2, n * 3).iterator());
		System.out.println(multipliedRDD.collect().toString());
	}

	public static void main(String... args) {
		JavaSparkContext context = SparkConfig.createSparkContext("local", "MapAndFilterExample");
		JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(1, 2, 3));

		MapAndFilterExample mapAndFilterExample = new MapAndFilterExample();
		mapAndFilterExample.map(numbersRDD);
		mapAndFilterExample.filter(numbersRDD);
		mapAndFilterExample.flatMap(numbersRDD);

		context.close();
	}
}
