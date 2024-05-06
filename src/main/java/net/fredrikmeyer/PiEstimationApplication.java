package net.fredrikmeyer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

import static net.fredrikmeyer.PiEstimationConstants.*;

public class PiEstimationApplication {
    private final KafkaStreams streams;

    public PiEstimationApplication(Properties properties) {
        Topology topology = PiEstimationTopology.getTopology();
        this.streams = new KafkaStreams(topology, properties);
    }

    public void start() {
        // Start producing random numbers
        new TupleGenerator(new RandomProducer()).start();

        // Consume estimations and publish on web socket
        new EstimationConsumer<>(JavalinApp::publishMessage, TOPIC_RANDOMS, new Tuple.TupleDeserializer()).start();
        new EstimationConsumer<>(JavalinApp::publishMessage,
                                 TOPIC_PI_ESTIMATION,
                                 Serdes.Double().deserializer()).start();
        new EstimationConsumer<>(JavalinApp::publishMessage, TOPIC_PI_ERROR, Serdes.Double().deserializer()).start();
        streams.start();
    }

    public void stop() {
        streams.close();
    }
}
