package net.fredrikmeyer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        final KafkaStreams streams = buildStreams();
        final var latch = new CountDownLatch(1);

        Runtime
                .getRuntime()
                .addShutdownHook(new Thread("streams-shutdown-hook") {
                    @Override
                    public void run() {
                        streams.close();
                        latch.countDown();
                    }
                });
        try {
            // Start app, serving on localhost:8081
            new JavalinApp().start();
            // Start producing random numbers
            new RandomProducer().start();

            new EstimationConsumer<>(JavalinApp::publishMessage, "randoms", new Tuple.TupleDeserializer()).start();
            new EstimationConsumer<>(JavalinApp::publishMessage, "pi-estimation", Serdes
                    .Double()
                    .deserializer()).start();
            new EstimationConsumer<>(JavalinApp::publishMessage, "pi-error", Serdes
                    .Double()
                    .deserializer()).start();

            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            System.exit(1);
        }
        System.exit(0);
    }

    private static KafkaStreams buildStreams() {
        Properties props = getProperties();
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Integer> randoms = builder
                .stream("randoms", Consumed.with(Serdes.String(), new Tuple.TupleSerde()))
                .mapValues(Tuple::insideCircle)
                .mapValues(v -> v ? 1 : 0);

        KStream<String, Double> fractionTable = getFractionStream(randoms);

        // Output result to a topic
        fractionTable.to("pi-estimation", Produced.with(Serdes.String(), Serdes.Double()));

        // Also make a topic with the error
        fractionTable
                .mapValues(v -> Math.abs(Math.PI - v) / Math.PI)
                .to("pi-error", Produced.with(Serdes.String(), Serdes.Double()));

        final Topology topology = builder.build();
        return new KafkaStreams(topology, props);
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pi-compute");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes
                .String()
                .getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes
                .String()
                .getClass());
        // To see updates more often when subscribing
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        return props;
    }

    private static KStream<String, Double> getFractionStream(KStream<String, Integer> stream) {
        return stream
                .groupByKey()
                .aggregate(() -> new FractionAggregator(0, 0),
                        (key, value, agg) -> new FractionAggregator(agg.trues() + value, agg.total() + 1), Materialized
                                .<String, FractionAggregator>as(Stores.persistentKeyValueStore("average-store"))
                                .withValueSerde(new FractionAggregator.FractionAggregatorSerde()))
                .toStream()
                .mapValues(avg -> avg.total() == 0 ? 0 : 4. * avg.trues() / avg.total());
    }

}