package net.fredrikmeyer.kafkapi;

import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import io.javalin.websocket.WsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque;

public class JavalinApp implements MessagePublisher {
    // Public only for tests... Is there a way to avoid this? (better code organization?)
    public final Javalin app;
    private static final Logger logger = LoggerFactory.getLogger(JavalinApp.class);
    private final Collection<WsContext> estimationContexts = new ConcurrentLinkedDeque<>();

    public JavalinApp() {
        this.app = Javalin.create(config -> {
            config.staticFiles.add("src/main/resources/public/", Location.EXTERNAL);

            config.router.mount(router -> {
                router.ws("/ws", ws -> {
                    ws.onConnect(estimationContexts::add);

                    ws.onClose(ctx -> {
                        estimationContexts.remove(ctx);
                        logger.info("Logged off: {}", ctx.reason());
                    });
                });
            });
        });
    }

    /**
     * Publish msg to clients currently listening on websockets.
     * @param msg The message to publish.
     */
    public void publishMessage(String msg) {
        estimationContexts.forEach(ctx -> {
            ctx.send(msg);
        });
    }

    public void start() {
        app.start(8081);
    }

    public void stop() {
        app.stop();
    }

}
