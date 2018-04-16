package com.mortgage.creditcheck.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

/**
 * ConsumeStreamedTopic will subscribe to the streamed output topic and send the
 * message into the success and failure topic based on the OCR engine data
 * validation
 * 
 * @author Vikas Singh
 *
 */

public class ConsumeStreamedTopic {
	public static final Logger LOGGER = LoggerFactory.getLogger(ConsumeStreamedTopic.class);

	@Value("${kafka.bootstrap-servers}")
	String brokerList = "localhost:9092,localhost:9093";
	
	// Threadpool of consumers
	private ExecutorService executor;
	final int numberOfThreads = 3;

	public void processStreamedTopic() {
		Consumer<String, String> consumer = new KafkaConsumer<>(getConsumerConfig());

		try {
			consumer.subscribe(Arrays.asList("DOC_STREAMED_TOPIC"));

			// Initialize a ThreadPool with size = 5 and use the BlockingQueue with size
			// =1000 to hold submitted tasks.
			executor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
					new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);

				for (ConsumerRecord<String, String> record : records) {

					executor.submit(new ConsumerThreadHandler(record));

					// get and validate the document from OCR Engine
					Random rand = new Random();
					int value = rand.nextInt(50);
					boolean response = callOCRAPI(value);
					if (response) {
						SendResponseToTopic("SUCCESS_CREDIT_TOPIC", record.key(), record.value());
					} else {
						SendResponseToTopic("FAILURE_CREDIT_TOPIC", record.key(), record.value());
					}
				}
				consumer.commitAsync();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}

	}

	private void SendResponseToTopic(String topic, String recordKey, String recordValue) {
		final Producer<String, String> producer = createProducer();
		try {
			final ProducerRecord<String, String> record = new ProducerRecord<>(topic, recordKey, recordValue);
			producer.send(record);
		} finally {
			producer.flush();
			producer.close();
		}

	}

	private Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new KafkaProducer<>(props);

	}

	private Boolean callOCRAPI(Integer a) throws IOException {

		/*
		 * URL url = new URL("localhost:8080/ocr/getdocument");
		 * 
		 * HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		 * connection.setDoOutput(true); connection.setDoInput(true);
		 * connection.setRequestMethod("POST");
		 * connection.setRequestProperty("Authorization", "Basic " +
		 * Base64.getEncoder().encodeToString(("username").getBytes()));
		 * connection.setRequestProperty("Content-Type", "application/json");
		 * OutputStream stream = connection.getOutputStream(); stream.close(); int
		 * httpCode = connection.getResponseCode();
		 * System.out.println("HTTP Response code: " + httpCode); // Success request if
		 * (httpCode == HttpURLConnection.HTTP_OK) { String jsonResponse =
		 * connection.getInputStream().toString(); System.out.println("json response > "
		 * + jsonResponse); }
		 */

		return a % 2 == 0;
	}

	private Properties getConsumerConfig() {
		Properties prop = new Properties();

		prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.put(ConsumerConfig.GROUP_ID_CONFIG, "LoanDataKafkaConsumer");
		prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
		prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		return prop;

	}

	public void shutdown() {
		if (executor != null) {
			executor.shutdown();
		}
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}

}
