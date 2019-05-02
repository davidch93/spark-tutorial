package com.dch.tutorial.spark.basic.rdd;

import com.dch.tutorial.spark.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Spark with Union.
 *
 * @author David.Christianto
 */
public class UnionAndDistinctExample {

    private static final Logger logger = LoggerFactory.getLogger(UnionAndDistinctExample.class);

    /**
     * Union value with {@link JavaRDD}.
     * <p>
     * Return the union of this RDD and another one.
     * </p>
     *
     * @param numbersRDD {@link JavaRDD}
     */
    private void union(JavaRDD<Integer> numbersRDD, JavaRDD<Integer> numbers2RDD) {
        JavaRDD<Integer> unionRDD = numbersRDD.union(numbers2RDD);
        logger.info("================== Union Result: " + unionRDD.collect().toString());
    }

    /**
     * Distinct value with {@link JavaRDD}.
     * <p>
     * Return a new RDD containing the distinct elements in this RDD.
     * </p>
     *
     * @param numbersRDD {@link JavaRDD}
     */
    private void distinct(JavaRDD<Integer> numbersRDD) {
        JavaRDD<Integer> distinctRDD = numbersRDD.distinct();
        logger.info("================== Distinct Result: " + distinctRDD.collect().toString());
    }

    public static void main(String... args) {
        JavaSparkContext context = SparkUtil.createSparkContext("local", "UnionAndDistinctExample");

        JavaRDD<Integer> numbers1RDD = context.parallelize(Arrays.asList(0, 5, 3, 0));
        JavaRDD<Integer> numbers2RDD = context.parallelize(Arrays.asList(8, 9, 3));

        UnionAndDistinctExample unionAndDistinctExample = new UnionAndDistinctExample();
        unionAndDistinctExample.union(numbers1RDD, numbers2RDD);
        unionAndDistinctExample.distinct(numbers1RDD);
    }
}
