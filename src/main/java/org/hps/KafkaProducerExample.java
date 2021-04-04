package org.hps;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaProducerExample {
    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);

    public static void main(String[] args) throws InterruptedException {
        KafkaProducerConfig config = KafkaProducerConfig.fromEnv();

        log.info(KafkaProducerConfig.class.getName() + ": {}", config.toString());

        Properties props = KafkaProducerConfig.createProperties(config);

        KafkaProducer producer = new KafkaProducer(props);
        log.info("Sending {} messages ...", config.getMessageCount());
        boolean blockProducer = System.getenv("BLOCKING_PRODUCER") != null;

        AtomicLong numSent = new AtomicLong(0);
        for (long i = 0; i < config.getMessageCount(); i++) {

            log.info("Sending messages \"" + config.getMessage() + " - {}\"{}", i);
            Future<RecordMetadata> recordMetadataFuture = producer.send(new ProducerRecord(config.getTopic(),
                    null, null,
                    UUID.randomUUID().toString(), "\"" + config.getMessage() + " - " + i));
            if(blockProducer) {
                try {
                    recordMetadataFuture.get();
                    // Increment number of sent messages only if ack is received by producer
                    numSent.incrementAndGet();
                } catch (ExecutionException e) {
                    log.warn("Message {} wasn't sent properly!", i, e.getCause());
                }
            } else {
                // Increment number of sent messages for non blocking producer
                numSent.incrementAndGet();
            }

            Thread.sleep(config.getDelay());
        }
        log.info("{} messages sent ...", numSent.get());
        producer.close();
        System.exit(numSent.get() == config.getMessageCount() ? 0 : 1);
    }
}
