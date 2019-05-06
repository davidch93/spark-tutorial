package com.dch.tutorial.spark.job;

import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Interface for all Spark Structure Streamer job.
 *
 * @author david.christianto
 */
public interface SparkStructureStreamerJob extends Serializable {

    /**
     * Method used to execute job.
     *
     * @param spark {@link SparkSession}.
     * @throws Exception if error occurred while executing the job.
     */
    void execute(SparkSession spark) throws Exception;
}
