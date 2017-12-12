package com.dch.tutorial.spark.actions;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dch.tutorial.spark.config.SparkConfig;

/**
 * Spark with Reduce actions.
 * 
 * @author David.Christianto
 */
public class ReduceExample {

	/**
	 * Reduce value with {@link JavaRDD}.
	 * <p>
	 * Reduces the elements of this RDD using the specified commutative and
	 * associative binary operator.
	 * </p>
	 */
	public void total(JavaRDD<Integer> numbersRDD) {
		long total = numbersRDD.reduce((n1, n2) -> n1 + n2);
		System.out.println(total);
	}

	public static void main(String... args) {
		JavaSparkContext context = SparkConfig.createSparkContext("local", "ReduceExample");
		JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(8, 0, 5, 3, 10, 6));

		ReduceExample reduceExample = new ReduceExample();
		reduceExample.total(numbersRDD);

		context.close();
	}
}
