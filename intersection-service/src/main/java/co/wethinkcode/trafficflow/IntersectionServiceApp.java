package co.wethinkcode.trafficflow;

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
import java.util.stream.Collectors;

public class IntersectionServiceApp {

    private static final int    PORT          = 7021;
    private static final String INGESTION_URL = "http://localhost:7020/intersections";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient   HTTP   = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    // Canonical list + lookup indexes, refreshed from ingestion.
    // volatile references, only ever reassigned (never mutated in place),
    // so request threads see a consistent snapshot without locking.
    private static volatile List<Intersection>            intersections = new ArrayList<>();
    private static volatile Map<String, Intersection>     byId          = new HashMap<>();
    private static volatile Map<String, List<Intersection>> byDistrict  = new HashMap<>();
    private static volatile boolean                       loaded        = false;

    // Wire DTO — matches ingestion-service's JSON shape.
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

        Javalin app = Javalin.create().start(PORT);

        app.get("/health", ctx -> {
            if (loaded) ctx.result("OK");
            else        ctx.status(503).result("INTERSECTION DATA NOT LOADED");
        });

        // Full list — debug / bulk consumer.
        app.get("/intersections", ctx -> {
            if (!loaded) { unavailable(ctx); return; }
            ctx.json(intersections);
        });

        // Primary lookup — routing-service uses this to validate endpoints.
        //   200 -> record  |  404 -> unknown id  |  503 -> data not loaded
        app.get("/intersections/{id}", ctx -> {
            if (!loaded) { unavailable(ctx); return; }
            String id = ctx.pathParam("id").toUpperCase();
            Intersection r = byId.get(id);
            if (r == null) {
                ctx.status(404).json(Map.of("error", "intersection not found: " + id));
                return;
            }
            ctx.json(r);
        });

        // Distinct district names.
        app.get("/districts", ctx -> {
            if (!loaded) { unavailable(ctx); return; }
            ctx.json(new ArrayList<>(byDistrict.keySet()));
        });

        // District lookup — case-insensitive.
        //   200 -> {"district":"Downtown","count":3,"intersectionIds":[...]}
        //   404 -> unknown district
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

        // Manual reload — recovers from bad start order, or after ingestion
        // data changes.
        app.post("/refresh", ctx -> {
            if (loadFromIngestion()) {
                ctx.json(Map.of("refreshed", true, "count", intersections.size()));
            } else {
                ctx.status(503).json(Map.of(
                        "refreshed", false,
                        "error",     "ingestion unreachable or returned bad data"));
            }
        });
    }

    private static void unavailable(io.javalin.http.Context ctx) {
        ctx.status(503).json(Map.of("error", "intersection data not loaded"));
    }

    /**
     * Pull the cleaned list from ingestion-service and rebuild indexes.
     * Synchronized so concurrent /refresh calls can't corrupt state.
     */
    private static synchronized boolean loadFromIngestion() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(INGESTION_URL))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                System.err.println("[intersection] ingestion returned HTTP "
                        + resp.statusCode());
                return false;
            }

            List<Intersection> list = MAPPER.readValue(
                    resp.body(),
                    MAPPER.getTypeFactory()
                            .constructCollectionType(List.class, Intersection.class));

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

    /** Same Title Case used by ingestion-service — keeps district lookups consistent. */
    private static String titleCase(String input) {
        if (input == null || input.isEmpty()) return null;
        String[] words = input.toLowerCase().split("\\s+");
        StringBuilder sb = new StringBuilder();
        for (String word : words) {
            if (word.isEmpty()) continue;
            if (sb.length() > 0) sb.append(' ');
            sb.append(Character.toUpperCase(word.charAt(0)));
            sb.append(word.substring(1));
        }
        return sb.toString();
    }
}