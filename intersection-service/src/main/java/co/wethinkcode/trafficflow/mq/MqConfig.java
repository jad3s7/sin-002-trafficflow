package co.wethinkcode.trafficflow;

public final class MqConfig {
    private MqConfig() {}

    public static final String BROKER_URL =
            envOr("MQ_BROKER_URL", "tcp://localhost:61616");
    public static final String CONGESTION_TOPIC =
            envOr("MQ_CONGESTION_TOPIC", "congestion-topic");
    public static final String HEARTBEAT_QUEUE =
            envOr("MQ_HEARTBEAT_QUEUE", "intersection-heartbeat-queue");

    /** How often intersection-service should publish a heartbeat. */
    public static final long HEARTBEAT_INTERVAL_SECONDS =
            Long.parseLong(envOr("MQ_HEARTBEAT_INTERVAL_SECONDS", "5"));

    /**
     * How long the watchdog waits without a heartbeat before alerting.
     * 3x interval tolerates one missed beat (transient blip) and fires
     * on three consecutive misses.
     */
    public static final long HEARTBEAT_TIMEOUT_SECONDS =
            Long.parseLong(envOr("MQ_HEARTBEAT_TIMEOUT_SECONDS", "15"));

    public static String brokerUrl() {
        return "failover:(" + BROKER_URL + ")";
    }

    private static String envOr(String name, String fallback) {
        String v = System.getenv(name);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}