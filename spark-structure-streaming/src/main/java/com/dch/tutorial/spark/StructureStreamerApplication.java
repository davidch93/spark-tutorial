package com.dch.tutorial.spark;

import com.dch.tutorial.spark.job.SparkStructureStreamerJob;
import com.dch.tutorial.spark.job.impl.SampleSparkStructureStreamerJob;
import org.apache.spark.sql.SparkSession;

/**
 * Spark structure streaming application.
 *
 * @author David.Christianto
 */
public class StructureStreamerApplication {

    public static void main(String... args) throws Exception {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[1]")
                .appName("spark_structure_streamer_job")
                .getOrCreate();
        SparkStructureStreamerJob structureStreamerJob = new SampleSparkStructureStreamerJob();
        structureStreamerJob.execute(sparkSession);
    }
}
