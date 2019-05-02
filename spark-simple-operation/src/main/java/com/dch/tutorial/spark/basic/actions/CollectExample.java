package com.dch.tutorial.spark.basic.actions;

import com.dch.tutorial.spark.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Spark with Collect action.
 *
 * @author David.Christianto
 */
public class CollectExample {

    private static final Logger logger = LoggerFactory.getLogger(CollectExample.class);

    /**
     * Collect value from {@link JavaRDD} to {@link List}.
     * <p>
     * Return an array that contains all of the elements in this RDD.
     * </p>
     *
     * @param numbersRDD {@link JavaRDD}
     */
    private void collect(JavaRDD<Integer> numbersRDD) {
        List<Integer> numbersList = numbersRDD.collect();
        logger.info("================== Result: " +
                numbersList.stream().map(String::valueOf).collect(Collectors.joining(",")));
    }

    public static void main(String... args) {
        JavaSparkContext context = SparkUtil.createSparkContext("local", "CollectExample");
        JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(0, 5, 3));

        CollectExample collectExample = new CollectExample();
        collectExample.collect(numbersRDD);
    }
}
