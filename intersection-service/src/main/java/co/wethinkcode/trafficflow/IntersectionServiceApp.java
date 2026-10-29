package co.wethinkcode.trafficflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class IntersectionServiceApp {

    private static final int    PORT          = 7021;
    private static final String INGESTION_URL = "http://localhost:7020/intersections";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient   HTTP   = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2)).build();

    private static volatile List<Intersection>              intersections = new ArrayList<>();
    private static volatile Map<String, Intersection>       byId          = new HashMap<>();
    private static volatile Map<String, List<Intersection>> byDistrict    = new HashMap<>();
    private static volatile boolean                         loaded        = false;

    // Heartbeat state — populated by the scheduler thread.
    private static ScheduledExecutorService heartbeatExec;
    private static volatile Connection      hbConn;
    private static volatile Session         hbSession;
    private static volatile MessageProducer hbProducer;
    private static final AtomicLong lastHeartbeatAt     = new AtomicLong(0);
    private static final AtomicLong lastHeartbeatFailAt = new AtomicLong(0);

    public static class Intersection {
        public String       id;
        public String       district;
        public String       signalType;
        public Boolean      active;
        public List<String> missingFields;
        public Intersection() {}
    }

    public static void main(String[] args) {
        // Bounded startup retry so start order doesn't matter in a demo.
        boolean ok = false;
        for (int attempt = 0; attempt < 3 && !ok; attempt++) {
            if (attempt > 0) {
                try { Thread.sleep(2000); } catch (InterruptedException ignored) { }
            }
            ok = loadFromIngestion();
        }
        if (!ok) {
            System.err.println("[intersection] WARNING: no data loaded at startup; "
                    + "use POST /refresh once ingestion is up");
        }

        startHeartbeat();
        Runtime.getRuntime().addShutdownHook(new Thread(
                IntersectionServiceApp::stopHeartbeat, "heartbeat-shutdown"));

        Javalin app = Javalin.create().start(PORT);

        // Error envelope (unchanged from Stage 2).
        app.exception(Exception.class, (e, ctx) -> {
            System.err.println("[intersection] unhandled exception: " + e);
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "internal server error"));
        });
        app.error(404, ctx -> ctx.status(404).json(
                Map.of("error", "not found", "path", ctx.path())));
        app.error(405, ctx -> ctx.status(405).json(
                Map.of("error", "method not allowed",
                        "method", ctx.req().getMethod(),
                        "path",   ctx.path())));

        app.get("/health", ctx -> {
            if (loaded) ctx.result("OK");
            else        ctx.status(503).result("INTERSECTION DATA NOT LOADED");
        });

        app.get("/intersections", ctx -> {
            if (!loaded) { unavailable(ctx); return; }
            ctx.json(intersections);
        });

        app.get("/intersections/{id}", ctx -> {
            if (!loaded) { unavailable(ctx); return; }
            String id = normalizeIntersectionId(ctx.pathParam("id"));
            Intersection r = byId.get(id);
            if (r == null) {
                ctx.status(404).json(Map.of("error", "intersection not found: " + id));
                return;
            }
            ctx.json(r);
        });

        app.get("/districts", ctx -> {
            if (!loaded) { unavailable(ctx); return; }
            ctx.json(new ArrayList<>(byDistrict.keySet()));
        });

        app.get("/districts/{name}", ctx -> {
            if (!loaded) { unavailable(ctx); return; }
            String requested = ctx.pathParam("name");
            String canonical = titleCase(requested);
            List<Intersection> members = canonical == null ? null : byDistrict.get(canonical);
            if (members == null) {
                ctx.status(404).json(Map.of("error", "district not found: " + requested));
                return;
            }
            Map<String, Object> resp = new HashMap<>();
            resp.put("district", canonical);
            resp.put("count", members.size());
            resp.put("intersectionIds",
                    members.stream().map(i -> i.id).collect(Collectors.toList()));
            ctx.json(resp);
        });

        app.post("/refresh", ctx -> {
            if (loadFromIngestion()) {
                ctx.json(Map.of("refreshed", true, "count", intersections.size()));
            } else {
                ctx.status(503).json(Map.of(
                        "refreshed", false,
                        "error",     "ingestion unreachable or returned bad data"));
            }
        });

        // Stage 4 debug endpoint — proves heartbeats are flowing without
        // needing to attach a consumer.
        app.get("/heartbeat", ctx -> {
            long last = lastHeartbeatAt.get();
            Map<String, Object> resp = new HashMap<>();
            resp.put("intervalSeconds", MqConfig.HEARTBEAT_INTERVAL_SECONDS);
            resp.put("timeoutSeconds",  MqConfig.HEARTBEAT_TIMEOUT_SECONDS);
            resp.put("connected",       hbProducer != null);
            if (last > 0) {
                resp.put("lastPublishedAtMsAgo", System.currentTimeMillis() - last);
            } else {
                resp.put("lastPublishedAtMsAgo", null);
                resp.put("warning", "no heartbeat published yet");
            }
            ctx.json(resp);
        });
    }

    // --- Heartbeat publisher --------------------------------------------

    private static void startHeartbeat() {
        heartbeatExec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-publisher");
            t.setDaemon(true);
            return t;
        });
        heartbeatExec.scheduleAtFixedRate(
                IntersectionServiceApp::publishHeartbeat,
                0,
                MqConfig.HEARTBEAT_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
    }

    /**
     * Runs on the heartbeat thread every HEARTBEAT_INTERVAL_SECONDS.
     * Synchronized so concurrent ticks (shouldn't happen, but defensive)
     * can't fight over connect/close. Failures are logged at most once
     * per minute so a down broker doesn't drown stdout.
     */
    private static synchronized void publishHeartbeat() {
        try {
            if (hbProducer == null) connectHeartbeat();
            String body = String.format(
                    "{\"service\":\"intersection-service\",\"ts\":%d,\"intersectionCount\":%d}",
                    System.currentTimeMillis(), intersections.size());
            TextMessage msg = hbSession.createTextMessage(body);
            hbProducer.send(msg);
            lastHeartbeatAt.set(System.currentTimeMillis());
        } catch (Exception e) {
            long now = System.currentTimeMillis();
            if (now - lastHeartbeatFailAt.get() > 60_000L) {
                System.err.println("[heartbeat] publish failed: " + e.getMessage()
                        + " — broker at " + MqConfig.BROKER_URL + " unreachable?");
                lastHeartbeatFailAt.set(now);
            }
            closeHeartbeat();  // force fresh connect on next tick
        }
    }

    private static void connectHeartbeat() throws JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
        Connection conn = cf.createConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(MqConfig.HEARTBEAT_QUEUE);
        MessageProducer producer = session.createProducer(queue);
        // Non-persistent: heartbeats are useless after they age out.
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        hbConn     = conn;
        hbSession  = session;
        hbProducer = producer;
        System.out.println("[heartbeat] connected to " + MqConfig.BROKER_URL
                + " queue=" + MqConfig.HEARTBEAT_QUEUE
                + " interval=" + MqConfig.HEARTBEAT_INTERVAL_SECONDS + "s");
    }

    private static synchronized void closeHeartbeat() {
        try { if (hbProducer != null) hbProducer.close(); } catch (Exception ignored) {}
        try { if (hbSession  != null) hbSession.close();  } catch (Exception ignored) {}
        try { if (hbConn     != null) hbConn.close();     } catch (Exception ignored) {}
        hbProducer = null;
        hbSession  = null;
        hbConn     = null;
    }

    private static void stopHeartbeat() {
        if (heartbeatExec != null) heartbeatExec.shutdownNow();
        closeHeartbeat();
    }

    // --- Data loading (unchanged from Stage 2) --------------------------

    private static synchronized boolean loadFromIngestion() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(INGESTION_URL))
                    .timeout(Duration.ofSeconds(5))
                    .GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                System.err.println("[intersection] ingestion returned HTTP "
                        + resp.statusCode());
                return false;
            }
            List<Intersection> list = MAPPER.readValue(resp.body(),
                    MAPPER.getTypeFactory().constructCollectionType(List.class, Intersection.class));

            Map<String, Intersection>       idx         = new HashMap<>();
            Map<String, List<Intersection>> districtIdx = new HashMap<>();
            for (Intersection r : list) {
                if (r.id != null) idx.put(r.id, r);
                if (r.district != null) {
                    districtIdx.computeIfAbsent(r.district, k -> new ArrayList<>()).add(r);
                }
            }

            intersections = list;
            byId          = idx;
            byDistrict    = districtIdx;
            loaded        = true;
            System.out.println("[intersection] loaded " + list.size()
                    + " intersections (" + idx.size() + " with ids, "
                    + districtIdx.size() + " districts)");
            return true;
        } catch (Exception e) {
            System.err.println("[intersection] load failed: " + e.getMessage());
            return false;
        }
    }

    private static void unavailable(io.javalin.http.Context ctx) {
        ctx.status(503).json(Map.of("error", "intersection data not loaded"));
    }

    private static String titleCase(String input) {
        if (input == null || input.isEmpty()) return null;
        String[] words = input.toLowerCase(Locale.ROOT).split("\\s+");
        StringBuilder sb = new StringBuilder();
        for (String word : words) {
            if (word.isEmpty()) continue;
            if (sb.length() > 0) sb.append(' ');
            sb.append(Character.toUpperCase(word.charAt(0)));
            sb.append(word.substring(1));
        }
        return sb.toString();
    }

    private static String normalizeIntersectionId(String input) {
        if (input == null) return null;
        String normalized = input.strip().toUpperCase(Locale.ROOT);
        return normalized.replaceFirst("^([A-Z]+)[\\s_-]*(\\d+)$", "$1-$2");
    }
}
