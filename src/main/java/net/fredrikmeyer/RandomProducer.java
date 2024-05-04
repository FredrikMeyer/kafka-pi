package net.fredrikmeyer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class RandomProducer implements Runnable {
    private final KafkaProducer<String, Tuple> producer;

    public RandomProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        var tupleSerde = new Tuple.TupleSerde();

        this.producer = new KafkaProducer<>(properties, new StringSerializer(), tupleSerde.serializer());

        System.out.println("XXX: " + producer.toString() + " YYY " + properties);
    }

    public void start() {
        Thread thread = new Thread(this);
        thread.start();
    }

    private ProducerRecord<String, Tuple> generateRandomRecord() {
        var randomTuple = new Random().doubles().map(d -> 2 * d - 1).limit(2).boxed().toList();

        return new ProducerRecord<>("randoms", "single-key", new Tuple(randomTuple.get(0), randomTuple.get(1)));
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                var record = generateRandomRecord();
                this.producer.send(record);

                Thread.sleep(10);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            producer.close();
        }
    }
}
