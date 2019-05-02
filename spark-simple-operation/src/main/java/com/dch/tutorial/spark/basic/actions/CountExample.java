package com.dch.tutorial.spark.basic.actions;

import com.dch.tutorial.spark.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Spark with Count actions.
 *
 * @author David.Christianto
 */
public class CountExample {

    private static final Logger logger = LoggerFactory.getLogger(CountExample.class);

    /**
     * Count values with {@link JavaRDD}.
     * <p>
     * Return the number of elements in the RDD.
     * </p>
     *
     * @param numbersRDD {@link JavaRDD}
     */
    private void count(JavaRDD<Integer> numbersRDD) {
        long numbersRDDSize = numbersRDD.count();
        logger.info("================== Size: " + numbersRDDSize);
    }

    public static void main(String... args) {
        JavaSparkContext context = SparkUtil.createSparkContext("local", "CountExample");
        JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(8, 0, 5, 3, 10, 6));

        CountExample countExample = new CountExample();
        countExample.count(numbersRDD);
    }
}
