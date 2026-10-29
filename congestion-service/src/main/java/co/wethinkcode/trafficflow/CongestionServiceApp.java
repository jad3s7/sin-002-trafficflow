package co.wethinkcode.trafficflow;

import io.javalin.Javalin;
import java.util.Map;

public class CongestionServiceApp {

    private static final int PORT      = 7022;
    private static final int MIN_LEVEL = 0;
    private static final int MAX_LEVEL = 8;

    // Current city-wide congestion level. 0 = free flowing, 8 = gridlocked.
    // volatile so reads on Javalin worker threads see the latest value;
    // writes go through setLevel(), which is synchronized so the
    // "did the level actually change?" check is atomic — that boolean is
    // exactly what stage 3 will use to publish to congestion-topic only
    // on a real change, not on every PUT.
    private static volatile int level = 0;

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(PORT);

        app.get("/health", ctx -> ctx.result("OK"));

        // Stage 2 contract: routing-service polls this per-request.
        // Shape matches the illustrative contract: {"level": 0-8}.
        // 'description' is a bonus for humans / logs; consumers should
        // only depend on 'level'.
        app.get("/congestion", ctx -> ctx.json(Map.of(
                "level",       level,
                "description", describe(level)
        )));

        // Admin / debug endpoint — the only way to change the level until
        // stage 3 wires the MQ publish. In a real system this would be
        // fed by sensor data; here it's exposed so the pipeline is
        // demonstrable end-to-end.
        app.put("/congestion", ctx -> {
            SetLevelRequest body;
            try {
                body = ctx.bodyAsClass(SetLevelRequest.class);
            } catch (Exception e) {
                ctx.status(400).json(Map.of("error", "invalid JSON body"));
                return;
            }
            if (body == null || body.level == null) {
                ctx.status(400).json(Map.of("error", "missing 'level'"));
                return;
            }
            if (body.level < MIN_LEVEL || body.level > MAX_LEVEL) {
                ctx.status(400).json(Map.of(
                        "error", "level must be between " + MIN_LEVEL + " and " + MAX_LEVEL,
                        "given", body.level));
                return;
            }
            boolean changed = setLevel(body.level);
            ctx.json(Map.of(
                    "level",       level,
                    "changed",     changed,
                    "description", describe(level)
            ));
        });
    }

    public static class SetLevelRequest {
        public Integer level;
    }

    /**
     * Update the level. Returns true iff the value actually changed — that's
     * the signal stage 3 will use to publish to congestion-topic.
     *
     * Synchronized so the read-compare-write is atomic even under
     * concurrent PUTs; the volatile field then makes the new value
     * immediately visible to GET /congestion.
     */
    private static synchronized boolean setLevel(int newLevel) {
        if (newLevel == level) return false;
        int previous = level;
        level = newLevel;
        System.out.println("[congestion] level " + previous + " -> " + newLevel
                + " (" + describe(newLevel) + ")");
        return true;
    }

    private static String describe(int l) {
        if (l <= 2) return "free flowing";
        if (l <= 5) return "moderate";
        return "heavy";
    }
}