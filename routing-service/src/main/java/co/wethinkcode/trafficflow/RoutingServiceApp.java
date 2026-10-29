package co.wethinkcode.trafficflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RoutingServiceApp {

    private static final int    PORT              = 7023;
    private static final String INTERSECTION_BASE = "http://localhost:7021";
    private static final String CONGESTION_BASE   = "http://localhost:7022";

    // Base travel time between two adjacent intersections, ignoring waits.
    // The final estimate is this whole expression multiplied by the live
    // congestion multiplier, so the output varies with live inputs.
    private static final double BASE_MINUTES = 5.0;

    // Expected wait at a signal, per endpoint. Keyed by the lowercase
    // signalType that ingestion normalizes to.
    private static final Map<String, Double> SIGNAL_DELAY = Map.of(
            "4-way",      2.0,
            "2-way",      1.0,
            "roundabout", 0.5);
    private static final double DEFAULT_SIGNAL_DELAY = 1.5;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient   HTTP   = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(PORT);
        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/route", ctx -> {
            String rawFrom = ctx.queryParam("from");
            String rawTo   = ctx.queryParam("to");
            if (rawFrom == null || rawFrom.isBlank()
                    || rawTo == null || rawTo.isBlank()) {
                ctx.status(400).json(Map.of("error", "query params 'from' and 'to' required"));
                return;
            }
            String from = rawFrom.trim().toUpperCase();
            String to   = rawTo.trim().toUpperCase();
            if (from.equals(to)) {
                ctx.status(400).json(Map.of("error", "'from' and 'to' must differ"));
                return;
            }

            // 1. Validate endpoints exist.
            Intersection fromIx, toIx;
            try {
                fromIx = fetchIntersection(from);
                if (fromIx == null) {
                    ctx.status(404).json(Map.of("error", "intersection not found: " + from));
                    return;
                }
                toIx = fetchIntersection(to);
                if (toIx == null) {
                    ctx.status(404).json(Map.of("error", "intersection not found: " + to));
                    return;
                }
            } catch (DependencyException e) {
                ctx.status(502).json(Map.of(
                        "error",  "intersection-service unavailable",
                        "detail", e.getMessage()));
                return;
            }

            // 2. Operational check — an inactive endpoint blocks the route.
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

            // 3. Current city-wide congestion.
            int level;
            try {
                level = fetchCongestionLevel();
            } catch (DependencyException e) {
                ctx.status(502).json(Map.of(
                        "error",  "congestion-service unavailable",
                        "detail", e.getMessage()));
                return;
            }

            // 4. Compute the estimate — depends on all live inputs above.
            double multiplier   = 1.0 + level * 0.25;
            double fromDelay    = signalDelay(fromIx.signalType);
            double toDelay      = signalDelay(toIx.signalType);
            double estimate     = (BASE_MINUTES + fromDelay + toDelay) * multiplier;

            List<String> warnings = new ArrayList<>();
            if (fromIx.active     == null) warnings.add("origin active status unknown");
            if (toIx.active       == null) warnings.add("destination active status unknown");
            if (fromIx.signalType == null) warnings.add("origin signal type unknown");
            if (toIx.signalType   == null) warnings.add("destination signal type unknown");

            Map<String, Object> breakdown = new HashMap<>();
            breakdown.put("baseMinutes",            BASE_MINUTES);
            breakdown.put("congestionLevel",        level);
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

    // --- Dependency calls ------------------------------------------------

    /** Returns null when intersection-service says 404. Throws on failure. */
    private static Intersection fetchIntersection(String id) throws DependencyException {
        HttpResponse<String> resp = get(INTERSECTION_BASE + "/intersections/" + id,
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

    private static int fetchCongestionLevel() throws DependencyException {
        HttpResponse<String> resp = get(CONGESTION_BASE + "/congestion",
                "congestion-service");
        if (resp.statusCode() != 200) {
            throw new DependencyException("HTTP " + resp.statusCode());
        }
        try {
            JsonNode root  = MAPPER.readTree(resp.body());
            JsonNode level = root.get("level");
            if (level == null || !level.isInt()) {
                throw new DependencyException("response missing integer 'level'");
            }
            return level.asInt();
        } catch (DependencyException e) {
            throw e;
        } catch (Exception e) {
            throw new DependencyException("bad JSON: " + e.getMessage());
        }
    }

    private static HttpResponse<String> get(String url, String name)
            throws DependencyException {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        try {
            return HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new DependencyException(name + " connection failed: " + e.getMessage());
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