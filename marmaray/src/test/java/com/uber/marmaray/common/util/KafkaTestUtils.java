package com.uber.marmaray.common.util;


import kafka.admin.AdminUtils;
import org.apache.spark.internal.Logging;

import java.util.Properties;

/**
 * This is a helper class for Kafka test suites. This has the functionality to set up
 * and tear down local Kafka servers, and to push data using Kafka producers.
 *
 * The reason to put Kafka test utility class in src is to test Python related Kafka APIs.
 */
public class KafkaTestUtils implements Logging {

    private Boolean brokerReady = false;

    String brokerAddress() {
        assert(brokerReady, "Kafka not setup yet or already torn down, cannot get broker address")
        return "$brokerHost:$brokerPort";
    }

    /** Create a Kafka topic and wait until it is propagated to the whole cluster */
    void createTopic(final String topic, final Integer partitions, final Properties config) {
        AdminUtils.createTopic(zkUtils, topic, partitions, 1, config);
        // wait until metadata is propagated
        (0 until partitions).foreach { p =>
            waitUntilMetadataIsPropagated(topic, p)
        }
    }

    /** Create a Kafka topic and wait until it is propagated to the whole cluster */
    def createTopic(topic: String, partitions: Int): Unit = {
        createTopic(topic, partitions, new Properties())
    }

    /** Create a Kafka topic and wait until it is propagated to the whole cluster */
    def createTopic(topic: String): Unit = {
        createTopic(topic, 1, new Properties())
    }
}