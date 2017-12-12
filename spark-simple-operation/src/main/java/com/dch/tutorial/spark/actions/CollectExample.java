package com.dch.tutorial.spark.actions;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dch.tutorial.spark.config.SparkConfig;

/**
 * Spark with Collect action.
 * 
 * @author David.Christianto
 */
public class CollectExample {

	/**
	 * Collect value from {@link JavaRDD} to {@link List}.
	 * <p>
	 * Return an array that contains all of the elements in this RDD.
	 * </p>
	 */
	public void collect(JavaRDD<Integer> numbersRDD) {
		List<Integer> numbersList = numbersRDD.collect();
		System.out.println(numbersList.toString());
	}

	public static void main(String... args) {
		JavaSparkContext context = SparkConfig.createSparkContext("local", "CollectExample");
		JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(0, 5, 3));

		CollectExample collectExample = new CollectExample();
		collectExample.collect(numbersRDD);

		context.close();
	}
}
