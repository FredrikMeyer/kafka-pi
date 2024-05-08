package net.fredrikmeyer.kafkapi;

import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        JavalinApp javalinApp = new JavalinApp();
        var piEstimationApplication = new PiEstimationApplication(getProperties(), javalinApp);
        final var latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                javalinApp.stop();
                piEstimationApplication.stop();
                latch.countDown();
            }
        });
        try {
            // Start app, serving on localhost:8081
            javalinApp.start();
            piEstimationApplication.start();
            latch.await();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            System.exit(1);
        }
        System.exit(0);
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pi-compute");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // To see updates more often when subscribing
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        return props;
    }
}