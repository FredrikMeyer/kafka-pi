package net.fredrikmeyer.kafkapi;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;

/**
 * Container class to keep track of true and total observations. Contains static classes for serialization and
 * deserialization.
 * @param trues Number of true observations.
 * @param total Total number of observations.
 */
public record FractionAggregator(long trues, long total) implements Serializable {
    public static FractionAggregator aggregate(FractionAggregator previous, Integer value) {
        return new FractionAggregator(previous.trues + value, previous.total() + 1);
    }

    static public class FractionAggregatorSerializer implements Serializer<FractionAggregator> {
        @Override
        public byte[] serialize(String topic, FractionAggregator data) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
                out.writeObject(data);
                out.flush();
                return bos.toByteArray();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class FractionAggregatorDeserializer implements Deserializer<FractionAggregator> {
        @Override
        public FractionAggregator deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            ByteArrayInputStream bis = new ByteArrayInputStream(data);

            try (ObjectInput in = new ObjectInputStream(bis)) {
                return (FractionAggregator) in.readObject();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class FractionAggregatorSerde extends Serdes.WrapperSerde<FractionAggregator> {
        public FractionAggregatorSerde() {
            super(new FractionAggregator.FractionAggregatorSerializer(),
                  new FractionAggregator.FractionAggregatorDeserializer());
        }

    }
    public FractionAggregator() {
        this(0, 0);
    }
}
