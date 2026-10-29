package co.wethinkcode.trafficflow;

/**
 * Shared MQ configuration. In this exercise each Maven project is
 * independent (no parent pom), so this class is physically present in
 * every service that needs it. The copy under common/ is the canonical
 * reference — keep the constants in sync across copies.
 */
public final class MqConfig {
    private MqConfig() {}

    public static final String BROKER_URL       = "tcp://localhost:61616";
    public static final String CONGESTION_TOPIC = "congestion-topic";
    public static final String HEARTBEAT_QUEUE  = "intersection-heartbeat-queue";
}