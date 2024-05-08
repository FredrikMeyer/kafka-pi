package net.fredrikmeyer.kafkapi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TupleSerializerTest {

    @Test
    void serialize_integers() {
        Tuple.TupleSerializer serializer = new Tuple.TupleSerializer();
        var original = new Tuple(3, 4);
        byte[] result = serializer.serialize("", original );

        var tupleDeserializer = new Tuple.TupleDeserializer();

        Tuple deserialized = tupleDeserializer.deserialize("", result);

        Assertions.assertEquals(original, deserialized);
    }

    @Test
    void serialize_doubles() {
        Tuple.TupleSerializer serializer = new Tuple.TupleSerializer();
        var original = new Tuple(0.1828128, 0.1989);
        byte[] result = serializer.serialize("", original );

        var tupleDeserializer = new Tuple.TupleDeserializer();

        Tuple deserialized = tupleDeserializer.deserialize("", result);

        Assertions.assertEquals(original, deserialized);
    }
}