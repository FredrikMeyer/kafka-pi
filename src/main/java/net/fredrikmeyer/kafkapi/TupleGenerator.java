package net.fredrikmeyer.kafkapi;

import java.util.Random;

/**
 * Produces an infinite stream of random tuples and sends them to a Kafka topic
 * via the TupleProcessor class.
 */
public class TupleGenerator implements Runnable {
    private final Random random;
    private final TupleProcessor processor;
    private final int MESSAGE_INTERVAL = 10;

    public TupleGenerator(TupleProcessor processor) {
        random = new Random();
        this.processor = processor;
    }

    public void start() {
        Thread thread = new Thread(this);
        thread.start();
    }

    public Tuple generateTuple() {
        var randomTuple = random.doubles().map(d -> 2 * d - 1).limit(2).boxed().toList();
        return new Tuple(randomTuple.get(0), randomTuple.get(1));
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                var tuple = generateTuple();
                this.processor.process(tuple);

                Thread.sleep(MESSAGE_INTERVAL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            processor.close();
        }
    }
}
