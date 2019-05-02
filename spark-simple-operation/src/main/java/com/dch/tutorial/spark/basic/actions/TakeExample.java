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
 * Spark with Take actions.
 *
 * @author David.Christianto
 */
public class TakeExample {

    private static final Logger logger = LoggerFactory.getLogger(TakeExample.class);

    /**
     * Take value from {@link JavaRDD} with numberOfTake.
     * <p>
     * Take the first num elements of the RDD. This currently scans the partitions
     * *one by one*, so it will be slow if a lot of partitions are required. In that
     * case, use collect() to get the whole RDD instead.
     * </p>
     *
     * @param numbersRDD   {@link JavaRDD}
     * @param numberOfTake Number of records to be take.
     */
    private void take(JavaRDD<Integer> numbersRDD, int numberOfTake) {
        List<Integer> numbersList = numbersRDD.take(numberOfTake);
        logger.info(String.format("================== Result take %d is: %s", numberOfTake,
                numbersList.stream().map(String::valueOf).collect(Collectors.joining(","))));
    }

    public static void main(String... args) {
        JavaSparkContext context = SparkUtil.createSparkContext("local", "TakeExample");
        JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(8, 0, 5, 3, 10, 6));

        TakeExample takeExample = new TakeExample();
        takeExample.take(numbersRDD, 3);
        takeExample.take(numbersRDD, 5);
    }
}
