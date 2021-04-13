package org.hps;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.StringTokenizer;

public class KafkaProducerConfig {
    private static final Logger log = LogManager.getLogger(KafkaProducerConfig.class);

    private static final long DEFAULT_MESSAGES_COUNT = 10;
    private static final String DEFAULT_MESSAGE = "Hello from Mazen Ezzeddine, Let's test assignors";
    private final String bootstrapServers;
    private final String topic;
    private final int delay;
    private final Long messageCount;
    private final String message;
    private final String acks;
    private final String headers;

    private final String additionalConfig;

    public KafkaProducerConfig(String bootstrapServers, String topic, int delay, Long messageCount, String message,
                               String acks, String additionalConfig, String headers) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.delay = delay;
        this.messageCount = messageCount;
        this.message = message;
        this.acks = acks;
        this.headers = headers;
        this.additionalConfig = additionalConfig;
    }

    public static KafkaProducerConfig fromEnv() {
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        String topic = System.getenv("TOPIC");
        int delay = Integer.valueOf(System.getenv("DELAY_MS"));
        Long messageCount = System.getenv("MESSAGE_COUNT") == null ? DEFAULT_MESSAGES_COUNT : Long.valueOf(System.getenv("MESSAGE_COUNT"));
        String message = System.getenv("MESSAGE") == null ? DEFAULT_MESSAGE : System.getenv("MESSAGE");

        String acks = System.getenv().getOrDefault("PRODUCER_ACKS", "1");
        String headers = System.getenv("HEADERS");
        String additionalConfig = System.getenv().getOrDefault("ADDITIONAL_CONFIG", "");

        return new KafkaProducerConfig(bootstrapServers, topic, delay, messageCount, message,
                acks, additionalConfig, headers);
    }

    public static Properties createProperties(KafkaProducerConfig config) {
        log.info("==================================================");
        log.info("Creating Properties");
        log.info("==================================================");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, config.getAcks());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        if (!config.getAdditionalConfig().isEmpty()) {
            StringTokenizer tok = new StringTokenizer(config.getAdditionalConfig(), ", \t\n\r");
            while (tok.hasMoreTokens()) {
                String record = tok.nextToken();
                int endIndex = record.indexOf('=');
                if (endIndex == -1) {
                    throw new RuntimeException("Failed to parse Map from String");
                }
                String key = record.substring(0, endIndex);
                String value = record.substring(endIndex + 1);
                props.put(key.trim(), value.trim());
            }
        }
        return props;
    }


    public String getBootstrapServers() {
        return bootstrapServers;
    }
    public String getTopic() {
        return topic;
    }
    public int getDelay() {
        return delay;
    }
    public Long getMessageCount() {
        return messageCount;
    }
    public String getMessage() {
        return message;
    }
    public String getAcks() {
        return acks;
    }

    public String getHeaders() {
        return headers;
    }
    public String getAdditionalConfig() {
        return additionalConfig;
    }
    @Override
    public String toString() {
        return "KafkaProducerConfig{" +
            "bootstrapServers='" + bootstrapServers + '\'' +
            ", topic='" + topic + '\'' +
            ", delay=" + delay +
            ", messageCount=" + messageCount +
            ", message='" + message + '\'' +
            ", acks='" + acks + '\'' +
            ", headers='" + headers + '\'' +
            ", additionalConfig='" + additionalConfig + '\'' +
            '}';
    }
}
