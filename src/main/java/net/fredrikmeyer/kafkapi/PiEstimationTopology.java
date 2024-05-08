package net.fredrikmeyer.kafkapi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import static net.fredrikmeyer.kafkapi.PiEstimationConstants.*;

public class PiEstimationTopology {

    public static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        var randoms = builder.stream(TOPIC_RANDOMS, Consumed.with(Serdes.String(), new Tuple.TupleSerde()));

        KStream<String, Double> fractionStream = getPiEstimationStream(randoms);

        // Output result to a topic
        fractionStream.to(TOPIC_PI_ESTIMATION, Produced.with(Serdes.String(), Serdes.Double()));

        // Also make a topic with the error
        fractionStream.mapValues(v -> Math.abs(Math.PI - v) / Math.PI)
                      .to(TOPIC_PI_ERROR, Produced.with(Serdes.String(), Serdes.Double()));

        return builder.build();
    }

    private static KStream<String, Double> getPiEstimationStream(KStream<String, Tuple> stream) {
        var fractionStore = Materialized.<String, FractionAggregator>as(
                                                Stores.persistentKeyValueStore("aggregate-store"))
                                        .withValueSerde(new FractionAggregator.FractionAggregatorSerde());

        return stream
                .mapValues(Tuple::insideCircle)
                .groupByKey()
                .aggregate(() -> new FractionAggregator(0, 0),
                           (key, value, agg) -> FractionAggregator.aggregate(
                                   agg,
                                   value ? 1 : 0),
                           fractionStore).toStream()
                .mapValues(PiEstimationTopology::getEstimate);
    }

    private static double getEstimate(FractionAggregator fractionAggregator) {
        return fractionAggregator.total() == 0
                ? 0
                : 4. * fractionAggregator.trues() / fractionAggregator.total();
    }
}
