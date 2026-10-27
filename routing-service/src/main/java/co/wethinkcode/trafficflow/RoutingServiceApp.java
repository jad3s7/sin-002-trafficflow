package co.wethinkcode.trafficflow;

import co.wethinkcode.trafficflow.model.RouteRequest;
import co.wethinkcode.trafficflow.model.RouteResponse;
import co.wethinkcode.trafficflow.service.RoutingService;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;

public class RoutingServiceApp {

    private static final String INTERSECTION_SERVICE_URL = "http://localhost:7021";
    private static final String CONGESTION_SERVICE_URL = "http://localhost:7022";

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7023);

        RoutingService routingService = new RoutingService(INTERSECTION_SERVICE_URL, CONGESTION_SERVICE_URL);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/api/route", ctx -> {
            String from = ctx.queryParam("from");
            String to = ctx.queryParam("to");

            if (from == null || to == null) {
                ctx.status(HttpStatus.BAD_REQUEST);
                ctx.result("Missing required parameters: from and to");
                return;
            }

            RouteRequest request = new RouteRequest(from, to);
            try {
                RouteResponse response = routingService.calculateRoute(request);
                ctx.json(response);
            } catch (IllegalArgumentException e) {
                ctx.status(HttpStatus.NOT_FOUND);
                ctx.result(e.getMessage());
            } catch (RuntimeException e) {
                ctx.status(HttpStatus.SERVICE_UNAVAILABLE);
                ctx.result("Dependency service unavailable: " + e.getMessage());
            }
        });
    }
}

// MQ TODO: subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig)
