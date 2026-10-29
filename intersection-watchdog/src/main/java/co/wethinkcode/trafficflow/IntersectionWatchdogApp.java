package co.wethinkcode.trafficflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMessage;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class IntersectionWatchdogApp {

    private static final int    PORT     = 7024;
    private static final String DLQ_NAME = "ActiveMQ.DLQ";
    private static final long   MQ_RETRY_INTERVAL_MS = 5_000L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Watchdog state.
    private static final long       watchdogStartedAt  = System.currentTimeMillis();
    private static final AtomicLong lastHeartbeatAt    = new AtomicLong(0);  // 0 = never
    private static final AtomicLong lastHeartbeatCount = new AtomicLong(0);  // intersectionCount
    private static final AtomicLong dlqCount           = new AtomicLong(0);
    private static final Set<String> heartbeatDlqIds = ConcurrentHashMap.newKeySet();
    private static volatile boolean alerting    = false;
    private static volatile String  alertReason = null;
    private static volatile long    alertSince  = 0;

    // MQ resources.
    private static volatile Connection      hbConn, dlqConn;
    private static volatile Session         hbSession, dlqSession;
    private static volatile MessageConsumer hbConsumer;
    private static volatile QueueBrowser    dlqBrowser;
    private static volatile long lastHeartbeatConnectAttemptAt;
    private static volatile long lastDlqConnectAttemptAt;

    private static ScheduledExecutorService checkExec;

    public static void main(String[] args) {
        startHeartbeatConsumer();
        startDlqWatcher();
        startChecker();
        Runtime.getRuntime().addShutdownHook(new Thread(
                IntersectionWatchdogApp::stopAll, "watchdog-shutdown"));

        Javalin app = Javalin.create().start(PORT);

        // Error envelope.
        app.exception(Exception.class, (e, ctx) -> {
            System.err.println("[watchdog] unhandled exception: " + e);
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "internal server error"));
        });
        app.error(404, ctx -> ctx.status(404).json(
                Map.of("error", "not found", "path", ctx.path())));
        app.error(405, ctx -> ctx.status(405).json(
                Map.of("error", "method not allowed")));

        // 200 = healthy. 503 = alerting (so any monitor polling /health
        // sees the alarm without needing to parse /alert).
        app.get("/health", ctx -> {
            if (alerting) {
                ctx.status(503).result("ALERT: " + alertReason);
            } else {
                ctx.result("OK");
            }
        });

        // Detailed alert state — the primary observable signal.
        app.get("/alert", ctx -> {
            long now      = System.currentTimeMillis();
            long lastBeat = lastHeartbeatAt.get();
            long since    = lastBeat > 0 ? (now - lastBeat) : (now - watchdogStartedAt);

            Map<String, Object> resp = new HashMap<>();
            resp.put("status",   alerting ? "alerting" : "healthy");
            resp.put("alerting", alerting);
            if (alerting) {
                resp.put("reason", alertReason);
                resp.put("alertSinceMsAgo", alertSince > 0 ? now - alertSince : null);
            }
            resp.put("timeoutSeconds",  MqConfig.HEARTBEAT_TIMEOUT_SECONDS);
            resp.put("intervalSeconds", MqConfig.HEARTBEAT_INTERVAL_SECONDS);
            resp.put("hasReceivedHeartbeat", lastBeat > 0);
            if (lastBeat > 0) {
                resp.put("lastHeartbeatAtMsAgo", lastBeat > 0 ? now - lastBeat : null);
                resp.put("lastHeartbeatIntersectionCount", lastHeartbeatCount.get());
            } else {
                resp.put("sinceWatchdogStartMsAgo", now - watchdogStartedAt);
            }
            resp.put("dlqMessageCount", dlqCount.get());
            ctx.json(resp);
        });

        // Debug-only: inject a tagged heartbeat dead-letter for the read-only
        // DLQ browser. This does not remove or consume broker DLQ messages.
        app.post("/inject-dlq", ctx -> {
            try (Connection conn = new ActiveMQConnectionFactory(MqConfig.BROKER_URL)
                    .createConnection()) {
                conn.start();
                try (Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                    Queue dlq = s.createQueue(DLQ_NAME);
                    try (MessageProducer p = s.createProducer(dlq)) {
                        TextMessage message = s.createTextMessage("test-dlq-message");
                        message.setStringProperty("trafficflow.originalDestination",
                                MqConfig.HEARTBEAT_QUEUE);
                        p.send(message);
                    }
                }
                ctx.json(Map.of("injected", true, "dlq", DLQ_NAME));
            } catch (Exception e) {
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });
    }

    // --- Heartbeat consumer ----------------------------------------------

    private static synchronized void startHeartbeatConsumer() {
        if (hbConsumer != null) return;
        lastHeartbeatConnectAttemptAt = System.currentTimeMillis();
        try {
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(MqConfig.brokerUrl());
            Connection conn = cf.createConnection();
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(MqConfig.HEARTBEAT_QUEUE);
            MessageConsumer consumer = session.createConsumer(queue);
            consumer.setMessageListener(message -> {
                try {
                    int count = -1;
                    if (message instanceof TextMessage) {
                        String body = ((TextMessage) message).getText();
                        // Best-effort intersectionCount extraction — parse
                        // failure does NOT disqualify the beat.
                        try {
                            var n = MAPPER.readTree(body).get("intersectionCount");
                            if (n != null && n.isInt()) count = n.asInt();
                        } catch (Exception ignored) { }
                    }
                    lastHeartbeatAt.set(System.currentTimeMillis());
                    lastHeartbeatCount.set(count);
                    // Quiet in the happy path — the /alert endpoint shows
                    // the live state; we don't need a log line per beat.
                } catch (Exception e) {
                    System.err.println("[watchdog] heartbeat handler error: "
                            + e.getMessage());
                }
            });
            hbConn     = conn;
            hbSession  = session;
            hbConsumer = consumer;
            System.out.println("[watchdog] consuming heartbeats from "
                    + MqConfig.HEARTBEAT_QUEUE);
        } catch (Exception e) {
            System.err.println("[watchdog] heartbeat consumer start failed: "
                    + e.getMessage());
        }
    }

    // --- DLQ watcher -----------------------------------------------------

    private static synchronized void startDlqWatcher() {
        if (dlqBrowser != null) return;
        lastDlqConnectAttemptAt = System.currentTimeMillis();
        try {
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(MqConfig.brokerUrl());
            Connection conn = cf.createConnection();
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue dlq = session.createQueue(DLQ_NAME);
            QueueBrowser browser = session.createBrowser(dlq);
            dlqConn     = conn;
            dlqSession  = session;
            dlqBrowser  = browser;
            System.out.println("[watchdog] browsing " + DLQ_NAME
                    + " for messages from " + MqConfig.HEARTBEAT_QUEUE);
        } catch (Exception e) {
            System.err.println("[watchdog] DLQ watcher start failed: "
                    + e.getMessage());
            closeDlqBrowser();
        }
    }

    // --- The check itself ------------------------------------------------

    private static void startChecker() {
        checkExec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "watchdog-checker");
            t.setDaemon(true);
            return t;
        });
        checkExec.scheduleAtFixedRate(IntersectionWatchdogApp::check, 0, 1, TimeUnit.SECONDS);
    }

    private static synchronized void check() {
        long now            = System.currentTimeMillis();
        reconnectConsumersIfNeeded(now);
        scanHeartbeatDeadLetters();
        long lastBeat       = lastHeartbeatAt.get();
        long sinceLastBeat  = lastBeat > 0 ? (now - lastBeat) : (now - watchdogStartedAt);
        long timeoutMs      = MqConfig.HEARTBEAT_TIMEOUT_SECONDS * 1000L;

        String newReason = null;
        if (sinceLastBeat > timeoutMs) {
            newReason = "heartbeat-timeout: "
                    + (lastBeat > 0
                    ? "no heartbeat for " + (sinceLastBeat / 1000) + "s"
                    : "no heartbeat received since watchdog started "
                    + (sinceLastBeat / 1000) + "s ago")
                    + " (timeout " + MqConfig.HEARTBEAT_TIMEOUT_SECONDS + "s)";
        } else if (dlqCount.get() > 0) {
            newReason = "dead-letter: " + dlqCount.get()
                    + " message(s) in " + DLQ_NAME;
        }

        boolean wasAlerting = alerting;
        alerting    = (newReason != null);
        alertReason = newReason;

        if (!wasAlerting && alerting) {
            alertSince = System.currentTimeMillis();
            System.err.println("[watchdog] ALERT RAISED: " + alertReason);
        } else if (wasAlerting && !alerting) {
            alertSince = 0;
            System.out.println("[watchdog] alert cleared — heartbeats flowing again");
        }
    }

    private static void reconnectConsumersIfNeeded(long now) {
        if (hbConsumer == null
                && now - lastHeartbeatConnectAttemptAt >= MQ_RETRY_INTERVAL_MS) {
            startHeartbeatConsumer();
        }
        if (dlqBrowser == null
                && now - lastDlqConnectAttemptAt >= MQ_RETRY_INTERVAL_MS) {
            startDlqWatcher();
        }
    }

    /** Browse the shared DLQ without consuming messages owned by operators or other services. */
    private static void scanHeartbeatDeadLetters() {
        QueueBrowser browser = dlqBrowser;
        if (browser == null) return;

        Set<String> currentIds = new HashSet<>();
        try {
            Enumeration<?> messages = browser.getEnumeration();
            while (messages.hasMoreElements()) {
                Object item = messages.nextElement();
                if (!(item instanceof Message)) continue;
                Message message = (Message) item;
                String originalDestination = originalDestination(message);
                if (!MqConfig.HEARTBEAT_QUEUE.equals(originalDestination)) continue;

                String messageId = message.getJMSMessageID();
                if (messageId != null) currentIds.add(messageId);
            }

            for (String id : currentIds) {
                if (!heartbeatDlqIds.contains(id)) {
                    System.err.println("[watchdog] ALERT: heartbeat dead-letter " + id);
                }
            }
            heartbeatDlqIds.clear();
            heartbeatDlqIds.addAll(currentIds);
            dlqCount.set(currentIds.size());
        } catch (Exception e) {
            System.err.println("[watchdog] DLQ browse failed: " + e.getMessage());
            closeDlqBrowser();
        }
    }

    private static String originalDestination(Message message) {
        if (message instanceof ActiveMQMessage) {
            try {
                var destination = ((ActiveMQMessage) message).getOriginalDestination();
                if (destination != null) return destination.getPhysicalName();
            } catch (Exception ignored) { }
        }
        try {
            if (message.propertyExists("trafficflow.originalDestination")) {
                return message.getStringProperty("trafficflow.originalDestination");
            }
        } catch (Exception ignored) { }
        return null;
    }

    private static synchronized void closeDlqBrowser() {
        try { if (dlqBrowser != null) dlqBrowser.close(); } catch (Exception ignored) { }
        try { if (dlqSession  != null) dlqSession.close();  } catch (Exception ignored) { }
        try { if (dlqConn     != null) dlqConn.close();     } catch (Exception ignored) { }
        dlqBrowser = null;
        dlqSession = null;
        dlqConn = null;
    }

    private static void stopAll() {
        if (checkExec != null) checkExec.shutdownNow();
        try { if (hbConsumer  != null) hbConsumer.close();  } catch (Exception ignored) { }
        try { if (hbSession   != null) hbSession.close();   } catch (Exception ignored) { }
        try { if (hbConn      != null) hbConn.close();      } catch (Exception ignored) { }
        closeDlqBrowser();
    }
}
