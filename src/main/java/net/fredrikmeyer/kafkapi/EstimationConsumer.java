package net.fredrikmeyer.kafkapi;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

public class EstimationConsumer<E> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(EstimationConsumer.class);
    private final org.apache.kafka.clients.consumer.Consumer<String, E> consumer;
    private final Consumer<String> endpoint;
    private final String topic;

    public EstimationConsumer(Consumer<String> endpoint, final String topic,
                              org.apache.kafka.clients.consumer.Consumer<String, E> consumer) {
        this.endpoint = endpoint;
        this.topic = topic;

        this.consumer = consumer;
    }

    public void start() {
        Thread thread = new Thread(this);
        thread.start();
    }

    @Override
    public void run() {
        logger.info("Subscribing to topic {}", topic);
        this.consumer.subscribe(Collections.singletonList(topic));
        var gson = new Gson();
        logger.info("Starting consumer run. Topic: {}", topic);
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, E> records = this.consumer.poll(Duration.ofMillis(100));

                StreamSupport.stream(records.spliterator(), false)
                             .map(record -> {
                                 JsonObject jsonObject = new JsonObject();
                                 jsonObject.addProperty("topic", topic);
                                 jsonObject.add("payload", gson.toJsonTree(record.value()));
                                 return jsonObject;
                             })
                             .forEach(json -> endpoint.accept(gson.toJson(json)));
            }
        } catch (RuntimeException e) {
            logger.info("Got exception: {}", e.toString());
        } finally {
            logger.info("Calling close.");
            this.consumer.close();
        }
    }
}
