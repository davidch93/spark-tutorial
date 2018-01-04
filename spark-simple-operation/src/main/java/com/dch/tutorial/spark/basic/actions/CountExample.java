package com.dch.tutorial.spark.basic.actions;

import com.dch.tutorial.spark.config.SparkConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Spark with Count actions.
 *
 * @author David.Christianto
 */
public class CountExample {

    /**
     * Count values with {@link JavaRDD}.
     * <p>
     * Return the number of elements in the RDD.
     * </p>
     */
    public void count(JavaRDD<Integer> numbersRDD) {
        long numbersRDDSize = numbersRDD.count();
        System.out.println(numbersRDDSize);
    }

    public static void main(String... args) {
        JavaSparkContext context = SparkConfig.createSparkContext("local", "CountExample");
        JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(8, 0, 5, 3, 10, 6));

        CountExample countExample = new CountExample();
        countExample.count(numbersRDD);

        context.close();
    }
}
