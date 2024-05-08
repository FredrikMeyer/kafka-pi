package net.fredrikmeyer.kafkapi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.function.Consumer;

class EstimationConsumerTest {
    private MockConsumer<String, String> consumer;
    private EstimationConsumer<String> estimationConsumer;
    private Consumer<String> mockCallback;

    private static final String TOPIC = "my-topic";
    private static final int PARTITION = 0;

    @BeforeEach
    void setup() {
        this.consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        this.mockCallback = (Consumer<String>) Mockito.mock(Consumer.class);
        estimationConsumer = new EstimationConsumer<>(this.mockCallback, TOPIC, consumer);
    }

    @Test
    void verify_that_callback_is_called_with_json_data() {
        // Arrange
        consumer.schedulePollTask(() -> {
            consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, PARTITION)));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, PARTITION, 0, "mikey", "value"));
        });

        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        startOffsets.put(tp, 0L);
        consumer.updateBeginningOffsets(startOffsets);

        // Act
        Thread thread = new Thread(estimationConsumer);
        thread.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            thread.interrupt();
        }

        // Verify
        Mockito.verify(mockCallback, Mockito.times(1)).accept("{\"topic\":\"my-topic\",\"payload\":\"value\"}");
    }
}