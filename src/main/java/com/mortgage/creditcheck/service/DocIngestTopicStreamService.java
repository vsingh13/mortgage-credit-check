package com.mortgage.creditcheck.service;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.beans.factory.annotation.Value;

/**
 * This is the streaming service class which will send the message from the
 * document ingest kafka topic to the streamed kafka topic.
 * 
 * @author Vikas Singh
 *
 */

public class DocIngestTopicStreamService {
	
	@Value("${kafka.bootstrap-servers}")
	String brokerList = "localhost:9092,localhost:9093";

	public void processStreamData() {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mortgage-credit-check");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
		
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		final Serde<String> stringSerde = Serdes.String();
		final Serde<byte[]> byteArraySerde = Serdes.ByteArray();
		final KStreamBuilder builder = new KStreamBuilder();
		final KStream<byte[], String> sourceDocument = builder.stream(byteArraySerde, stringSerde, "DOC_INGEST_TOPIC");
		final KStream<byte[], String> streamed = sourceDocument
				.map((key, value) -> KeyValue.pair(key, value.toUpperCase()));
		streamed.to(byteArraySerde, stringSerde, "DOC_STREAMED_TOPIC");
		final KafkaStreams streams = new KafkaStreams(builder, props);
		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
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
