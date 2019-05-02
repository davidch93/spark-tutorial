package com.dch.tutorial.spark.basic.rdd;

import com.dch.tutorial.spark.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Spark with Map, Filter and FlatMap.
 *
 * @author David.Christianto
 */
public class MapAndFilterExample {

    private static final Logger logger = LoggerFactory.getLogger(MapAndFilterExample.class);

    /**
     * Map value with {@link Function} n * n.
     * <p>
     * Return a new RDD by applying a function to all elements of this RDD.
     * </p>
     *
     * @param numbersRDD {@link JavaRDD}
     */
    private void map(JavaRDD<Integer> numbersRDD) {
        JavaRDD<Integer> squaresRDD = numbersRDD.map(n -> n * n);
        logger.info("================== Map Result: " + squaresRDD.collect().toString());
    }

    /**
     * Filter value with {@link Function} n % 2 == 0.
     * <p>
     * Return a new RDD containing only the elements that satisfy a predicate.
     * </p>
     *
     * @param numbersRDD {@link JavaRDD}
     */
    private void filter(JavaRDD<Integer> numbersRDD) {
        JavaRDD<Integer> evenRDD = numbersRDD.filter(n -> n % 2 == 0);
        logger.info("================== Filter Result: " + evenRDD.collect().toString());
    }

    /**
     * Map value with {@link FlatMapFunction} n, n * 2, n * 3.
     * <p>
     * Return a new RDD by first applying a function to all elements of this RDD,
     * and then flattening the results.
     * </p>
     *
     * @param numbersRDD {@link JavaRDD}
     */
    private void flatMap(JavaRDD<Integer> numbersRDD) {
        JavaRDD<Integer> multipliedRDD = numbersRDD.flatMap(n -> Arrays.asList(n, n * 2, n * 3).iterator());
        logger.info("================== Flat Map Result: " + multipliedRDD.collect().toString());
    }

    /**
     * Filter value with {@link Function} n % 2 != 0 then map value with {@link Function} n * n.
     *
     * @param numbersRDD {@link JavaRDD}
     */
    private void filterThenMap(JavaRDD<Integer> numbersRDD) {
        JavaRDD<Integer> resultRDD = numbersRDD.filter(n -> n % 2 != 0).map(n -> n * n);
        logger.info("================== Result: " + resultRDD.collect().toString());
    }

    public static void main(String... args) {
        JavaSparkContext context = SparkUtil.createSparkContext("local", "MapAndFilterExample");
        JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(1, 2, 3));

        MapAndFilterExample mapAndFilterExample = new MapAndFilterExample();
        mapAndFilterExample.map(numbersRDD);
        mapAndFilterExample.filter(numbersRDD);
        mapAndFilterExample.flatMap(numbersRDD);
        mapAndFilterExample.filterThenMap(numbersRDD);
    }
}
