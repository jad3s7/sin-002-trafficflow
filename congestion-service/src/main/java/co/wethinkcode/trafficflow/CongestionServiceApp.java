package co.wethinkcode.trafficflow;

import io.javalin.Javalin;

public class CongestionServiceApp {

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7022);

        app.get("/health", ctx -> ctx.result("OK"));

        // Congestion level endpoint
        app.get("/congestion-level", ctx -> {
            CongestionLevel response = new CongestionLevel();
            response.congestion_level = 4; // Simple integer between 0 and 8
            ctx.status(200).json(response);
        });
    }

    // Data class for congestion level response with public fields for Jackson serialization
    public static class CongestionLevel {
        public int congestion_level;
    }
}

// MQ TODO: publishes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig)
