package co.wethinkcode.trafficflow.service;

import co.wethinkcode.trafficflow.model.RouteRequest;
import co.wethinkcode.trafficflow.model.RouteResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class RoutingService {

    // --- ETA model constants -------------------------------------------------
    private static final int BASE_TIME_MINUTES = 10;
    private static final int MINUTES_PER_CONGESTION_LEVEL = 2;
    private static final int CLOSED_INTERSECTION_PENALTY_MINUTES = 30;

//
//    int baseTime = 10;
//    int estimatedTravelTime = baseTime + (congestionLevel * 2);
    // --- HTTP client ---------------------------------------------------------
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);

    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .build();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final String intersectionServiceUrl;
    private final String congestionServiceUrl;

    public RoutingService(String intersectionServiceUrl, String congestionServiceUrl) {
        this.intersectionServiceUrl = intersectionServiceUrl;
        this.congestionServiceUrl = congestionServiceUrl;
    }

    public RouteResponse calculateRoute(RouteRequest request) {
        // Both upstream calls still happen over HTTP.
        IntersectionInfo fromInfo = validateIntersection(request.from);
        IntersectionInfo toInfo   = validateIntersection(request.to);
        int congestionLevel       = getCongestionLevel();

//        int estimatedTravelTime = baseTime + (congestionLevel * 2);

        // ETA = base + congestion term + validity term
        int congestionMinutes = congestionLevel * MINUTES_PER_CONGESTION_LEVEL;
        int validityMinutes   = fromInfo.penaltyMinutes + toInfo.penaltyMinutes;
        int estimatedTravelTime = BASE_TIME_MINUTES + congestionMinutes + validityMinutes;

        return new RouteResponse(
                request.from,
                request.to,
                congestionLevel,
                estimatedTravelTime,
                deriveStatus(fromInfo, toInfo)
        );
    }

    // ------------------------------------------------------------------------
    // Intersection lookup
    // ------------------------------------------------------------------------
//    private void validateIntersection(String intersectionId) { ... }
    private IntersectionInfo validateIntersection(String intersectionId) {
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(intersectionServiceUrl + "/intersections/" + intersectionId))
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new RuntimeException("Error validating intersection: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while validating intersection", e);
        }

        if (response.statusCode() == 404) {
            // Preserved original behaviour: missing intersection is fatal.
            throw new IllegalArgumentException("Intersection not found: " + intersectionId);
        }
        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to validate intersection: HTTP " + response.statusCode());
        }

        return parseIntersection(intersectionId, response.body());
    }

    /**
     * Parse whatever the intersection service returns and extract a validity
     * penalty. Unknown / empty bodies are treated as "open, no penalty".
     *
     * Recognised fields (all optional, any combination):
     *   "closed": true                 -> adds CLOSED_INTERSECTION_PENALTY_MINUTES
     *   "status": "closed"|"blocked"   -> adds CLOSED_INTERSECTION_PENALTY_MINUTES
     *   "valid": false                 -> adds CLOSED_INTERSECTION_PENALTY_MINUTES
     *   "penaltyMinutes": <int>        -> added verbatim
     *   "delayMinutes":   <int>        -> added verbatim
     */
    private IntersectionInfo parseIntersection(String intersectionId, String body) {
        boolean closed = false;
        int penaltyMinutes = 0;

        try {
            JsonNode node = objectMapper.readTree(body);
            if (node != null && !node.isMissingNode()) {
                if (node.hasNonNull("closed") && node.get("closed").asBoolean()) {
                    closed = true;
                }
                if (node.hasNonNull("valid") && !node.get("valid").asBoolean()) {
                    closed = true;
                }
                if (node.hasNonNull("status")) {
                    String s = node.get("status").asText("");
                    if ("closed".equalsIgnoreCase(s) || "blocked".equalsIgnoreCase(s)
                            || "invalid".equalsIgnoreCase(s)) {
                        closed = true;
                    }
                }
                if (node.hasNonNull("penaltyMinutes")) {
                    penaltyMinutes += Math.max(0, node.get("penaltyMinutes").asInt(0));
                }
                if (node.hasNonNull("delayMinutes")) {
                    penaltyMinutes += Math.max(0, node.get("delayMinutes").asInt(0));
                }
            }
        } catch (IOException e) {
            // Not JSON / unparseable -> treat as "open, no extra info"
        }

        if (closed) {
            penaltyMinutes += CLOSED_INTERSECTION_PENALTY_MINUTES;
        }

        return new IntersectionInfo(intersectionId, closed, penaltyMinutes);
    }

    // ------------------------------------------------------------------------
    // Congestion lookup
    // ------------------------------------------------------------------------
    private int getCongestionLevel() {
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(congestionServiceUrl + "/congestion"))
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new RuntimeException("Error getting congestion level: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while getting congestion level", e);
        }

        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to get congestion level: HTTP " + response.statusCode());
        }

        try {
            CongestionResponse congestionResponse =
                    objectMapper.readValue(response.body(), CongestionResponse.class);
            return congestionResponse.level;
        } catch (IOException e) {
            throw new RuntimeException("Error parsing congestion level: " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------------
    // Status derivation
    // ------------------------------------------------------------------------
    private static String deriveStatus(IntersectionInfo from, IntersectionInfo to) {
        if (from.closed || to.closed) {
            return "restricted";
        }
        if (from.penaltyMinutes > 0 || to.penaltyMinutes > 0) {
            return "restricted";
        }
        return "valid";
    }

    // ------------------------------------------------------------------------
    // DTOs
    // ------------------------------------------------------------------------
    private static class CongestionResponse {
        public int level;
    }

    public static class IntersectionInfo {
        public final String id;
        public final boolean closed;
        public final int penaltyMinutes;

        public IntersectionInfo(String id, boolean closed, int penaltyMinutes) {
            this.id = id;
            this.closed = closed;
            this.penaltyMinutes = penaltyMinutes;
        }
    }
}