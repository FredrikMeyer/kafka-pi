package net.fredrikmeyer.kafkapi;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class TupleProcessorTest {
    @Test
    public void test_produces_random_numberS() {
        MockProducer<String, Tuple> mockProducer = new MockProducer<>(true,
                                                                      new StringSerializer(),
                                                                      new Tuple.TupleSerializer());

        Tuple tuple = new Tuple(0.3, 0.2);
        TupleProcessor tupleProcessor = new TupleProcessor(mockProducer);

        tupleProcessor.process(tuple);

        assertEquals(mockProducer.history().size(), 1);
        assertEquals(mockProducer.history().getFirst().value(), tuple);
    }

}