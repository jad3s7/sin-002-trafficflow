package co.wethinkcode.trafficflow;

import io.javalin.Javalin;

public class CongestionServiceApp {

    private static int currentCongestionLevel = 4;

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7022);

        app.get("/health", ctx -> ctx.result("OK"));

        // GET /congestion - returns current city-wide congestion level (0-8)
        app.get("/congestion", ctx -> {
            CongestionResponse response = new CongestionResponse();
            response.level = currentCongestionLevel;
            ctx.json(response);
        });

        // POST /congestion - allows updating the congestion level (for testing/demo)
        app.post("/congestion", ctx -> {
            CongestionRequest request = ctx.bodyAsClass(CongestionRequest.class);
            if (request.level < 0 || request.level > 8) {
                ctx.status(400).result("Congestion level must be between 0 and 8");
                return;
            }
            currentCongestionLevel = request.level;
            CongestionResponse response = new CongestionResponse();
            response.level = currentCongestionLevel;
            ctx.json(response);
        });
    }

    // Request model for updating congestion level
    public static class CongestionRequest {
        public int level;
    }

    // Response model for congestion level
    public static class CongestionResponse {
        public int level;
    }
}

// MQ TODO: publishes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig)
