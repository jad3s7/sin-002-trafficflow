package co.wethinkcode.trafficflow;

/** Shared MQ configuration — keep in sync with common/MqConfig.java. */
public final class MqConfig {
    private MqConfig() {}

    public static final String BROKER_URL       = "tcp://localhost:61616";
    public static final String CONGESTION_TOPIC = "congestion-topic";
    public static final String HEARTBEAT_QUEUE  = "intersection-heartbeat-queue";
}