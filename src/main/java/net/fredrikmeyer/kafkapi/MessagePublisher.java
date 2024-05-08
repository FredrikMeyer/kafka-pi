package net.fredrikmeyer.kafkapi;

public interface MessagePublisher {
    <E> void publishMessage(E msg);
}
