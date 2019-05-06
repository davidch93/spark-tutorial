package com.dch.tutorial.spark.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Util class that provides function to manipulate JSON.
 *
 * @author david.christianto
 */
public class JsonUtil {

    /**
     * Default config for {@link ObjectMapper}
     */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Method used to convert json string to {@link JsonNode}.
     *
     * @param data Json string.
     * @return {@link JsonNode}
     * @see ObjectMapper#readTree(String)
     */
    public static JsonNode toJsonNode(String data) {
        if (data == null || data.isEmpty())
            throw new IllegalArgumentException("String data can't be empty!");

        try {
            return objectMapper.readTree(data);
        } catch (IOException ex) {
            throw new RuntimeException("Invalid JSON string!", ex);
        }
    }
}
