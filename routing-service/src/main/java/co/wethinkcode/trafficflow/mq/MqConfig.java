package co.wethinkcode.trafficflow;

/**
 * Shared MQ configuration. In this exercise each Maven project is
 * independent (no parent pom), so this class is physically present in
 * every service that needs it. The copy under common/ is the canonical
 * reference — keep the constants in sync across copies.
 *
 * Values are read from environment variables at startup, with sensible
 * defaults so the demo works without any env setup. No MQ string
 * literals are permitted anywhere else in the codebase; every service
 * references these constants.
 */
public final class MqConfig {
    private MqConfig() {}

    /**
     * Raw broker URL. Publishers use this directly so a broker failure
     * surfaces as an immediate publish failure (reported to the caller),
     * not a silent failover retry.
     */
    public static final String BROKER_URL =
            envOr("MQ_BROKER_URL", "tcp://localhost:61616");

    /** Topic that congestion-service publishes level changes to. */
    public static final String CONGESTION_TOPIC =
            envOr("MQ_CONGESTION_TOPIC", "congestion-topic");

    /** Queue that intersection-service publishes heartbeats to. */
    public static final String HEARTBEAT_QUEUE =
            envOr("MQ_HEARTBEAT_QUEUE", "intersection-heartbeat-queue");

    /**
     * Broker URL wrapped in ActiveMQ's failover: transport. Long-lived
     * subscribers use this so they reconnect transparently if the broker
     * restarts. The transport prefix lives here (not scattered in each
     * subscriber) so the failover choice is a single config decision.
     */
    public static String brokerUrl() {
        return "failover:(" + BROKER_URL + ")";
    }

    private static String envOr(String name, String fallback) {
        String v = System.getenv(name);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}