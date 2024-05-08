package net.fredrikmeyer.kafkapi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PiEstimationTopologyTest {
    private TestInputTopic<String, Tuple> inputTopic;
    private TestOutputTopic<String, Double> estimationTopic;
    private TestOutputTopic<String, Double> errorTopic;

    @BeforeEach
    public void init() throws IOException {
        Properties props = new Properties();

        String tmpDir = Files.createTempDirectory("kafka-test").toAbsolutePath().toString();
        props.put(StreamsConfig.STATE_DIR_CONFIG, tmpDir);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        TopologyTestDriver testDriver = new TopologyTestDriver(PiEstimationTopology.getTopology(),
                                                               props,
                                                               Instant.now());


        System.out.println(PiEstimationTopology.getTopology().describe());

        inputTopic = testDriver.createInputTopic(PiEstimationConstants.TOPIC_RANDOMS,
                                                 Serdes.String().serializer(),
                                                 new Tuple.TupleSerde().serializer());
        estimationTopic = testDriver.createOutputTopic(PiEstimationConstants.TOPIC_PI_ESTIMATION,
                                                       Serdes.String().deserializer(),
                                                       Serdes.Double().deserializer());
        errorTopic = testDriver.createOutputTopic(PiEstimationConstants.TOPIC_PI_ERROR,
                                                  Serdes.String().deserializer(),
                                                  Serdes.Double().deserializer());

    }

    @Test
    public void test_get_somewhat_close_to_pi() {
        Random random = new Random(3159);
        for (int i = 0; i < 1000; i++) {
            inputTopic.pipeInput("", new Tuple(random.nextDouble(), random.nextDouble()));
        }

        Double lastValue = estimationTopic.readValuesToList().getLast();
        assertEquals(Math.PI, lastValue, 0.02);
        assertEquals(0.0056, errorTopic.readValuesToList().getLast(), 0.0001);
    }

}