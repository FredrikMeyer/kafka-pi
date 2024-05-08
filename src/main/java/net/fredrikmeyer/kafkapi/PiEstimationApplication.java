package net.fredrikmeyer.kafkapi;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

import static net.fredrikmeyer.kafkapi.PiEstimationConstants.*;

public class PiEstimationApplication {
    private final KafkaStreams streams;
    private final MessagePublisher messagePublisher;

    public PiEstimationApplication(Properties properties, MessagePublisher messagePublisher) {
        Topology topology = PiEstimationTopology.getTopology();
        this.streams = new KafkaStreams(topology, properties);
        this.messagePublisher = messagePublisher;
    }

    public void start() {
        // Start producing random numbers
        new TupleGenerator(new RandomProducer()).start();

        // Consume estimations and publish on web socket
        new EstimationConsumer<>(messagePublisher::publishMessage, TOPIC_RANDOMS, Tuple.getConsumer(
                PiEstimationConstants.consumerProperties("random-consumer"))).start();

        new EstimationConsumer<>(messagePublisher::publishMessage, TOPIC_PI_ESTIMATION,
                                 new KafkaConsumer<>(consumerProperties("estimation-consumer"),
                                                     Serdes.String().deserializer(),
                                                     Serdes.Double().deserializer())).start();
        new EstimationConsumer<>(messagePublisher::publishMessage, TOPIC_PI_ERROR,
                                 new KafkaConsumer<>(consumerProperties("error-consumer"),
                                                     Serdes.String().deserializer(),
                                                     Serdes.Double().deserializer())).start();
        streams.start();
    }

    public void stop() {
        streams.close();
    }
}
