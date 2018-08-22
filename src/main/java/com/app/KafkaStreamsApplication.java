package com.app;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import javax.annotation.PostConstruct;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamsApplication implements CommandLineRunner
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApplication.class);

    @PostConstruct
    public void logSomething() {
        logger.info("Kafka Streams Spring Boot application logger");
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsApplication.class, args).close();
    }

    @Override
    public void run(String... args) throws Exception
    {
        logger.info("Starting kafka streams application ...........");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kst");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // build topology
        StreamsBuilder builder = new StreamsBuilder();

        // create source stream from source topic
        builder.stream("inventory-in").to("inventory-out");

        Topology topology = builder.build();
        logger.info(topology.describe().toString());

        KafkaStreams streams = new KafkaStreams(topology, props);

        // attach shutdown handler to catch control-c
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
