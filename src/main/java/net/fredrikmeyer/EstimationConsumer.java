package net.fredrikmeyer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

public class EstimationConsumer<E> implements Runnable {
    private final KafkaConsumer<String, E> kafkaConsumer;
    private final Consumer<String> endpoint;
    private final String topic;

    public EstimationConsumer(Consumer<String> endpoint, final String topic, Deserializer<E> deserializer) {
        this.endpoint = endpoint;
        this.topic = topic;
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var kafkaConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), deserializer);
        this.kafkaConsumer = kafkaConsumer;

        kafkaConsumer.subscribe(Collections.singletonList(topic));
    }

    public void start() {
        Thread thread = new Thread(this);
        thread.start();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            ConsumerRecords<String, E> records = this.kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, E> record : records) {
                var gson = new Gson();
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("topic", topic);
                jsonObject.add("payload", gson.toJsonTree(record.value()));

                endpoint.accept(gson.toJson(jsonObject));
            }
        }
        this.kafkaConsumer.close();
    }
}
