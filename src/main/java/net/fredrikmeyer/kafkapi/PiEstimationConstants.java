package net.fredrikmeyer.kafkapi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;

public class PiEstimationConstants {
    final static String TOPIC_RANDOMS = "randoms";
    final static String TOPIC_PI_ESTIMATION = "pi-estimation";
    final static String TOPIC_PI_ERROR = "pi-error";

    public static @NotNull Properties consumerProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
