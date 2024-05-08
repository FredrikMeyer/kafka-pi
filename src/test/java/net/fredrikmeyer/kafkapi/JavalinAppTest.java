package net.fredrikmeyer.kafkapi;

import io.javalin.testtools.JavalinTest;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JavalinAppTest {
    @Test
    public void test() {
        // Stupid test :) It just tests that Javalin works...
        var app = new JavalinApp();
        JavalinTest.test(app.app, (server, client) -> {
            var request = new Request.Builder()
                    .addHeader("sec-websocket-key",
                               "8WRGe9bP70KfD9u3cLJ/dA==")
                    .addHeader("sec-websocket-version", "13")
                    .addHeader("upgrade", "websocket")
                    .addHeader("connection", "upgrade");

            Response response = client.request("/ws", request);
            assertEquals(101, response.code());
        });
    }
}