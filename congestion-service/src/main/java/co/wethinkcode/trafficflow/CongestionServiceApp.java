package co.wethinkcode.trafficflow;

import io.javalin.Javalin;
import java.util.Map;

public class CongestionServiceApp {

    private static final int PORT      = 7022;
    private static final int MIN_LEVEL = 0;
    private static final int MAX_LEVEL = 8;

    private static volatile int level = 0;

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(PORT);

        // Catch-all error envelope.
        app.exception(Exception.class, (e, ctx) -> {
            System.err.println("[congestion] unhandled exception: " + e);
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

        app.get("/congestion", ctx -> ctx.json(Map.of(
                "level",       level,
                "description", describe(level)
        )));

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