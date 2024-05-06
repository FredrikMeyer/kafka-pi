package net.fredrikmeyer;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;

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


    public Tuple() {
        this(0, 0);
    }
}
