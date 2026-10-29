package co.wethinkcode.trafficflow;

/**
 * Watchdog's local copy of the MQ settings. Each service is an independent
 * Maven project, so this class is duplicated in the participating modules.
 */
public final class MqConfig {

    public static final String BROKER_URL = envOr("MQ_BROKER_URL", "tcp://localhost:61616");
    public static final String HEARTBEAT_QUEUE =
            envOr("MQ_HEARTBEAT_QUEUE", "intersection-heartbeat-queue");
    public static final long HEARTBEAT_INTERVAL_SECONDS =
            Long.parseLong(envOr("MQ_HEARTBEAT_INTERVAL_SECONDS", "5"));
    public static final long HEARTBEAT_TIMEOUT_SECONDS =
            Long.parseLong(envOr("MQ_HEARTBEAT_TIMEOUT_SECONDS", "15"));

    public static String brokerUrl() {
        return "failover:(" + BROKER_URL + ")";
    }

    private static String envOr(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private MqConfig() {
    }
}
