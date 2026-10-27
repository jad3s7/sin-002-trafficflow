package co.wethinkcode.trafficflow;

import io.javalin.Javalin;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

public class IntersectionServiceApp {

    private static List<Intersection> intersections = new ArrayList<>();
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static class Intersection {
        String id;
        String district;
        String signalType;
        Boolean active;

        Intersection() {}

        Intersection(String id, String district, String signalType, Boolean active) {
            this.id = id;
            this.district = district;
            this.signalType = signalType;
            this.active = active;
        }

        public String getId() { return id; }
        public String getDistrict() { return district; }
        public String getSignalType() { return signalType; }
        public Boolean getActive() { return active; }
    }

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7021);

        app.get("/health", ctx -> ctx.result("OK"));

        // Load intersection data from ingestion-service on startup
        loadIntersectionsFromIngestionService();

        // Domain endpoint: validate/look up an intersection by ID
        app.get("/intersections/{id}", ctx -> {
            String id = ctx.pathParam("id").toUpperCase();
            Intersection intersection = findIntersectionById(id);
            if (intersection != null) {
                ctx.json(intersection);
            } else {
                ctx.status(404).result("Intersection not found: " + id);
            }
        });

        // Domain endpoint: validate/look up a district
        app.get("/districts/{name}", ctx -> {
            String districtName = toTitleCase(ctx.pathParam("name"));
            List<Intersection> districtIntersections = findIntersectionsByDistrict(districtName);
            if (!districtIntersections.isEmpty()) {
                ctx.json(districtIntersections);
            } else {
                ctx.status(404).result("District not found: " + districtName);
            }
        });
    }

    private static void loadIntersectionsFromIngestionService() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:7020/intersections"))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                intersections = objectMapper.readValue(response.body(), new TypeReference<List<Intersection>>() {});
                System.out.println("Loaded " + intersections.size() + " intersections from ingestion-service");
            } else {
                System.err.println("Failed to load intersections: HTTP " + response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Error loading intersections from ingestion-service: " + e.getMessage());
        }
    }

    private static Intersection findIntersectionById(String id) {
        return intersections.stream()
                .filter(i -> i.id != null && i.id.equalsIgnoreCase(id))
                .findFirst()
                .orElse(null);
    }

    private static List<Intersection> findIntersectionsByDistrict(String districtName) {
        List<Intersection> result = new ArrayList<>();
        for (Intersection i : intersections) {
            if (i.district != null && i.district.equalsIgnoreCase(districtName)) {
                result.add(i);
            }
        }
        return result;
    }

    private static String toTitleCase(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        return input.substring(0, 1).toUpperCase() + input.substring(1).toLowerCase();
    }
}

// MQ TODO: publishes a periodic heartbeat to ActiveMQ queue MqConfig.HEARTBEAT_QUEUE at
// MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig), consumed by intersection-watchdog.
