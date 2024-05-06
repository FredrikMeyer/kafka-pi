package net.fredrikmeyer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.state.Stores;

import static net.fredrikmeyer.PiEstimationConstants.*;

public class PiEstimationTopology {

    public static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Integer> randoms = builder.stream(TOPIC_RANDOMS,
                                                          Consumed.with(Serdes.String(), new Tuple.TupleSerde()))
                                                  .mapValues(Tuple::insideCircle).mapValues(v -> v ? 1 : 0);

        KStream<String, Double> fractionTable = getFractionStream(randoms);

        // Output result to a topic
        fractionTable.to(TOPIC_PI_ESTIMATION, Produced.with(Serdes.String(), Serdes.Double()));

        // Also make a topic with the error
        fractionTable.mapValues(v -> Math.abs(Math.PI - v) / Math.PI)
                     .to(TOPIC_PI_ERROR, Produced.with(Serdes.String(), Serdes.Double()));

        return builder.build();
    }

    private static KStream<String, Double> getFractionStream(KStream<String, Integer> stream) {
        var fractionStore = Materialized.<String, FractionAggregator>as(Stores.persistentKeyValueStore("average-store"))
                                        .withValueSerde(new FractionAggregator.FractionAggregatorSerde());

        return stream.groupByKey().aggregate(() -> new FractionAggregator(0, 0),
                                             (key, value, agg) -> FractionAggregator.aggregate(agg, value),
                                             fractionStore).toStream().mapValues(FractionAggregator::getScaledFraction);
    }
}
