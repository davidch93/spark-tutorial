package com.dch.tutorial.spark.basic.actions;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dch.tutorial.spark.config.SparkConfig;

/**
 * Spark with Take actions.
 * 
 * @author David.Christianto
 */
public class TakeExample {

	/**
	 * Take value from {@link JavaRDD} with index.
	 * <p>
	 * Take the first num elements of the RDD. This currently scans the partitions
	 * *one by one*, so it will be slow if a lot of partitions are required. In that
	 * case, use collect() to get the whole RDD instead.
	 * </p>
	 */
	public void take(JavaRDD<Integer> numbersRDD, int index) {
		List<Integer> numbersList = numbersRDD.take(index);
		System.out.println(numbersList.toString());
	}

	public static void main(String... args) {
		JavaSparkContext context = SparkConfig.createSparkContext("local", "TakeExample");
		JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(8, 0, 5, 3, 10, 6));

		TakeExample takeExample = new TakeExample();
		takeExample.take(numbersRDD, 3);

		context.close();
	}
}
