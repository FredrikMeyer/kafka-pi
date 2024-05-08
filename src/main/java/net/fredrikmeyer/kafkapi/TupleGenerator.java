package net.fredrikmeyer.kafkapi;

import java.util.Random;

public class TupleGenerator implements Runnable {
    private final Random random;
    private final RandomProducer producer;
    private final int MESSAGE_INTERVAL = 10;

    public TupleGenerator(RandomProducer producer) {
        random = new Random();
        this.producer = producer;
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
                this.producer.process(tuple);

                Thread.sleep(MESSAGE_INTERVAL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            producer.close();
        }
    }
}
