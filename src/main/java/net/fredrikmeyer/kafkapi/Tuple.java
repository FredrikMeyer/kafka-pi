package net.fredrikmeyer.kafkapi;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.util.Properties;

public record Tuple(double x, double y) implements Serializable {

    public boolean insideCircle() {
        return Math.pow(x, 2) + Math.pow(y, 2) < 1;
    }

    public static class TupleDeserializer implements Deserializer<Tuple> {
        @Override
        public Tuple deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            ByteArrayInputStream bis = new ByteArrayInputStream(data);

            try (ObjectInput in = new ObjectInputStream(bis)) {
                return (Tuple) in.readObject();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class TupleSerde extends Serdes.WrapperSerde<Tuple> {
        public TupleSerde() {
            super(new Tuple.TupleSerializer(), new Tuple.TupleDeserializer());
        }
    }

    public static class TupleSerializer implements Serializer<Tuple> {
        @Override
        public byte[] serialize(String topic, Tuple data) {
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

    public static Consumer<String, Tuple> getConsumer(Properties properties) {
        return new KafkaConsumer<>(properties, new StringDeserializer(), new TupleDeserializer());
    }
}
