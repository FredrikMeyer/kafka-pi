package net.fredrikmeyer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;

import static net.fredrikmeyer.PiEstimationConstants.TOPIC_RANDOMS;

public class RandomProducer {
    private final Producer<String, Tuple> producer;

    public RandomProducer() {
        Properties properties = getProperties();
        var tupleSerde = new Tuple.TupleSerde();
        this.producer = new KafkaProducer<>(properties, new StringSerializer(), tupleSerde.serializer());
    }

    public RandomProducer(Producer<String, Tuple> producer) {
        this.producer = producer;
    }

    public void process(Tuple tuple) {
        ProducerRecord<String, Tuple> record = new ProducerRecord<>(TOPIC_RANDOMS, "some-key", tuple);
        producer.send(record);
    }

    private static @NotNull Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public void close() {
        producer.close();
    }
}
