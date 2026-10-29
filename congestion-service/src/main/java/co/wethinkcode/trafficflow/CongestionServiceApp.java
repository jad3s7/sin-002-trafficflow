package co.wethinkcode.trafficflow;

import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import java.util.HashMap;
import java.util.Map;

public class CongestionServiceApp {

    private static final int PORT      = 7022;
    private static final int MIN_LEVEL = 0;
    private static final int MAX_LEVEL = 8;

    private static volatile int level = 0;

    // MQ publish state — nullable; lazily (re)created by ensurePublisher().
    private static volatile Connection      mqConn     = null;
    private static volatile Session         mqSession  = null;
    private static volatile MessageProducer mqProducer = null;

    public static void main(String[] args) {
        // Best-effort connect at startup; publishLevel() retries if this
        // failed and the broker came up later.
        ensurePublisher();

        // Clean shutdown so the broker sees the disconnect immediately.
        Runtime.getRuntime().addShutdownHook(new Thread(
                CongestionServiceApp::closePublisher, "mq-shutdown"));

        Javalin app = Javalin.create().start(PORT);

        // Error envelope (unchanged from Stage 2).
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

            // Set + publish atomically so concurrent PUTs can't publish
            // an older value after a newer one.
            PublishResult result = applyLevel(body.level);

            Map<String, Object> resp = new HashMap<>();
            resp.put("level",       level);
            resp.put("changed",     result.changed);
            resp.put("published",   result.published);
            resp.put("description", describe(level));
            if (result.changed && !result.published) {
                resp.put("warning", "level changed but publish to "
                        + MqConfig.CONGESTION_TOPIC + " failed — broker down?");
            }
            ctx.json(resp);
        });
    }

    public static class SetLevelRequest {
        public Integer level;
    }

    static class PublishResult {
        final boolean changed;
        final boolean published;
        PublishResult(boolean changed, boolean published) {
            this.changed = changed;
            this.published = published;
        }
    }

    private static synchronized PublishResult applyLevel(int newLevel) {
        boolean changed = (newLevel != level);
        if (changed) {
            int previous = level;
            level = newLevel;
            System.out.println("[congestion] level " + previous + " -> " + newLevel
                    + " (" + describe(newLevel) + ")");
        }
        boolean published = changed && publishLevel(newLevel);
        return new PublishResult(changed, published);
    }

    // --- MQ plumbing -----------------------------------------------------

    private static synchronized boolean ensurePublisher() {
        if (mqProducer != null) return true;
        try {
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
            Connection conn = cf.createConnection();
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(MqConfig.CONGESTION_TOPIC);
            MessageProducer producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            mqConn     = conn;
            mqSession  = session;
            mqProducer = producer;
            System.out.println("[congestion] connected to broker " + MqConfig.BROKER_URL
                    + " (topic=" + MqConfig.CONGESTION_TOPIC + ")");
            return true;
        } catch (Exception e) {
            System.err.println("[congestion] broker unavailable at " + MqConfig.BROKER_URL
                    + ": " + e.getMessage());
            return false;
        }
    }

    private static boolean publishLevel(int newLevel) {
        if (!ensurePublisher()) return false;
        try {
            TextMessage msg = mqSession.createTextMessage("{\"level\":" + newLevel + "}");
            mqProducer.send(msg);
            System.out.println("[congestion] published {\"level\":" + newLevel
                    + "} to " + MqConfig.CONGESTION_TOPIC);
            return true;
        } catch (JMSException e) {
            System.err.println("[congestion] publish failed: " + e.getMessage());
            closePublisher();  // force reconnect on next attempt
            return false;
        }
    }

    private static synchronized void closePublisher() {
        try { if (mqProducer != null) mqProducer.close(); } catch (Exception ignored) {}
        try { if (mqSession  != null) mqSession.close();  } catch (Exception ignored) {}
        try { if (mqConn     != null) mqConn.close();     } catch (Exception ignored) {}
        mqProducer = null;
        mqSession  = null;
        mqConn     = null;
    }

    private static String describe(int l) {
        if (l <= 2) return "free flowing";
        if (l <= 5) return "moderate";
        return "heavy";
    }
}