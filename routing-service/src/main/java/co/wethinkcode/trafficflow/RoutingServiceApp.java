package co.wethinkcode.trafficflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RoutingServiceApp {

    private static final int    PORT              = 7023;
    private static final String INTERSECTION_BASE = "http://localhost:7021";

    private static final double BASE_MINUTES = 5.0;

    private static final Map<String, Double> SIGNAL_DELAY = Map.of(
            "4-way",      2.0,
            "2-way",      1.0,
            "roundabout", 0.5);
    private static final double DEFAULT_SIGNAL_DELAY = 1.5;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient   HTTP   = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2)).build();

    // --- Congestion state, fed by the MQ subscriber ---------------------
    // levelLastUpdatedAt == 0 means "no message ever received" -> stale.
    private static volatile int    level              = 0;
    private static volatile long   levelLastUpdatedAt = 0L;

    // Subscriber resources, kept so the shutdown hook can close them.
    private static volatile Connection      mqConn;
    private static volatile Session         mqSession;
    private static volatile MessageConsumer mqConsumer;

    public static void main(String[] args) {
        startSubscriber();
        Runtime.getRuntime().addShutdownHook(new Thread(
                RoutingServiceApp::stopSubscriber, "mq-shutdown"));

        Javalin app = Javalin.create().start(PORT);

        // Error envelope.
        app.exception(Exception.class, (e, ctx) -> {
            System.err.println("[routing] unhandled exception: " + e);
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "internal server error"));
        });
        app.error(404, ctx -> ctx.status(404).json(
                Map.of("error", "not found", "path", ctx.path())));
        app.error(405, ctx -> ctx.status(405).json(
                Map.of("error", "method not allowed",
                        "method", ctx.req().getMethod(),
                        "path",   ctx.path())));

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/route", ctx -> {
            String rawFrom = ctx.queryParam("from");
            String rawTo   = ctx.queryParam("to");
            if (rawFrom == null || rawFrom.isBlank()
                    || rawTo   == null || rawTo.isBlank()) {
                ctx.status(400).json(Map.of(
                        "error", "query params 'from' and 'to' required"));
                return;
            }
            String from = rawFrom.trim().toUpperCase();
            String to   = rawTo.trim().toUpperCase();
            if (from.equals(to)) {
                ctx.status(400).json(Map.of(
                        "error", "'from' and 'to' must differ"));
                return;
            }

            Intersection fromIx, toIx;
            try {
                fromIx = fetchIntersection(from);
                if (fromIx == null) {
                    ctx.status(404).json(Map.of(
                            "error", "intersection not found: " + from));
                    return;
                }
                toIx = fetchIntersection(to);
                if (toIx == null) {
                    ctx.status(404).json(Map.of(
                            "error", "intersection not found: " + to));
                    return;
                }
            } catch (DependencyException e) {
                ctx.status(502).json(Map.of(
                        "error",  "intersection-service unavailable",
                        "detail", e.getMessage()));
                return;
            }

            if (Boolean.FALSE.equals(fromIx.active)) {
                ctx.status(409).json(Map.of(
                        "error", "origin intersection is inactive: " + from));
                return;
            }
            if (Boolean.FALSE.equals(toIx.active)) {
                ctx.status(409).json(Map.of(
                        "error", "destination intersection is inactive: " + to));
                return;
            }

            // Congestion now comes from the subscriber cache, not an HTTP GET.
            int    levelSnapshot = level;
            long   lastUpdated   = levelLastUpdatedAt;
            boolean stale        = (lastUpdated == 0L);

            double multiplier = 1.0 + levelSnapshot * 0.25;
            double fromDelay  = signalDelay(fromIx.signalType);
            double toDelay    = signalDelay(toIx.signalType);
            double estimate   = (BASE_MINUTES + fromDelay + toDelay) * multiplier;

            List<String> warnings = new ArrayList<>();
            if (fromIx.active     == null) warnings.add("origin active status unknown");
            if (toIx.active       == null) warnings.add("destination active status unknown");
            if (fromIx.signalType == null) warnings.add("origin signal type unknown");
            if (toIx.signalType   == null) warnings.add("destination signal type unknown");
            if (stale) {
                warnings.add("congestion level not yet received from "
                        + MqConfig.CONGESTION_TOPIC + " — using default 0");
            }

            Map<String, Object> breakdown = new HashMap<>();
            breakdown.put("baseMinutes",            BASE_MINUTES);
            breakdown.put("congestionLevel",        levelSnapshot);
            breakdown.put("congestionStale",        stale);
            if (!stale) {
                breakdown.put("congestionUpdatedAtMsAgo",
                        System.currentTimeMillis() - lastUpdated);
            }
            breakdown.put("congestionMultiplier",   multiplier);
            breakdown.put("fromSignalDelayMinutes", fromDelay);
            breakdown.put("toSignalDelayMinutes",   toDelay);

            Map<String, Object> resp = new HashMap<>();
            resp.put("from",             from);
            resp.put("to",               to);
            resp.put("estimatedMinutes", Math.round(estimate * 10.0) / 10.0);
            resp.put("breakdown",        breakdown);
            if (!warnings.isEmpty()) resp.put("warnings", warnings);
            ctx.json(resp);
        });
    }

    // --- MQ subscriber ---------------------------------------------------

    private static void startSubscriber() {
        try {
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
                    MqConfig.brokerUrl());
            Connection conn = cf.createConnection();
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(MqConfig.CONGESTION_TOPIC);
            MessageConsumer consumer = session.createConsumer(topic);

            // Runs on ActiveMQ's dispatch thread. Writes only volatile
            // scalars; readers on Javalin threads see the latest value
            // without locking.
            consumer.setMessageListener(message -> {
                try {
                    if (!(message instanceof TextMessage)) return;
                    String body = ((TextMessage) message).getText();
                    JsonNode root = MAPPER.readTree(body);
                    JsonNode lvl  = root.get("level");
                    if (lvl == null || !lvl.isInt()) {
                        System.err.println("[routing] ignoring malformed message: "
                                + body);
                        return;
                    }
                    int newLevel = lvl.asInt();
                    if (newLevel < 0 || newLevel > 8) {
                        System.err.println("[routing] ignoring out-of-range level "
                                + newLevel);
                        return;
                    }
                    level              = newLevel;
                    levelLastUpdatedAt = System.currentTimeMillis();
                    System.out.println("[routing] congestion level = " + newLevel);
                } catch (Exception e) {
                    System.err.println("[routing] message handler error: "
                            + e.getMessage());
                }
            });

            mqConn     = conn;
            mqSession  = session;
            mqConsumer = consumer;

            System.out.println("[routing] subscribed to " + MqConfig.CONGESTION_TOPIC
                    + " at " + MqConfig.BROKER_URL);
        } catch (Exception e) {
            System.err.println("[routing] subscriber start failed: " + e.getMessage()
                    + " — routing-service will serve with stale congestion until"
                    + " the broker is reachable");
        }
    }

    private static void stopSubscriber() {
        try { if (mqConsumer != null) mqConsumer.close(); } catch (Exception ignored) {}
        try { if (mqSession  != null) mqSession.close();  } catch (Exception ignored) {}
        try { if (mqConn     != null) mqConn.close();     } catch (Exception ignored) {}
    }

    // --- Intersection-service calls (unchanged) -------------------------

    private static Intersection fetchIntersection(String id) throws DependencyException {
        String encoded = URLEncoder.encode(id, StandardCharsets.UTF_8);
        HttpResponse<String> resp = get(
                INTERSECTION_BASE + "/intersections/" + encoded,
                "intersection-service");
        if (resp.statusCode() == 404) return null;
        if (resp.statusCode() == 503) {
            throw new DependencyException("intersection data not loaded downstream");
        }
        if (resp.statusCode() != 200) {
            throw new DependencyException("HTTP " + resp.statusCode());
        }
        try {
            return MAPPER.readValue(resp.body(), Intersection.class);
        } catch (Exception e) {
            throw new DependencyException("bad JSON: " + e.getMessage());
        }
    }

    private static HttpResponse<String> get(String url, String name)
            throws DependencyException {
        HttpRequest req;
        try {
            req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .GET().build();
        } catch (IllegalArgumentException e) {
            throw new DependencyException(name + " request URL malformed: "
                    + e.getMessage());
        }
        try {
            return HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new DependencyException(name + " connection failed: "
                    + e.getMessage());
        }
    }

    // --- Helpers ---------------------------------------------------------

    private static double signalDelay(String signalType) {
        if (signalType == null) return DEFAULT_SIGNAL_DELAY;
        Double d = SIGNAL_DELAY.get(signalType.toLowerCase());
        return d != null ? d : DEFAULT_SIGNAL_DELAY;
    }

    // --- DTOs & exceptions -----------------------------------------------

    public static class Intersection {
        public String       id;
        public String       district;
        public String       signalType;
        public Boolean      active;
        public List<String> missingFields;
        public Intersection() {}
    }

    static class DependencyException extends Exception {
        DependencyException(String msg) { super(msg); }
    }
}