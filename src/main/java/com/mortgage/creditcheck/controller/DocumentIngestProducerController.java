package com.mortgage.creditcheck.controller;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Document ingest controller will expose a rest API to push the doc uplodaded
 * into the kafka topic
 * 
 * @author Vikas Singh
 *
 */

@RestController
@RequestMapping(value = "/creditcheck")
public class DocumentIngestProducerController {
	
	@Value("${kafka.bootstrap-servers}")
	String brokerList = "localhost:9092,localhost:9093";

	@RequestMapping(path = "/producer", method = RequestMethod.POST)
	public ResponseEntity sendMessageToTopic(@RequestBody String filestream,
			@PathVariable(value = "fileType", required = true) String fileType,
			@RequestHeader(value = "AuthToken", required = true) String authToken,
			@RequestHeader(value = "customerRef", required = true) String customerRef) throws Exception {
		final Producer<String, String> docIngestProducer = createProducer();

		try {

			final ProducerRecord<String, String> record = new ProducerRecord<>("DOC_INGEST_TOPIC",
					customerRef + "-" + fileType, filestream);

			docIngestProducer.send(record, ((metadata, exception) -> {

				if (metadata != null) {
					System.out.println("Record the data event sent --> " + record.key() + " | " + record.value() + " | "
							+ metadata.partition());
				} else {

					System.out.println("Error Sending record data event --> " + record.value());
				}
			}));

		} finally {
			docIngestProducer.flush();
			docIngestProducer.close();
		}
		return ResponseEntity.ok("success");

	}

	private Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "DocumentIngest");
		// - max.in.flight.requests.per.connection (default 5)
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
		// Set the number of retries - retries
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		// Request timeout - request.timeout.ms
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
		// Only retry after one second.
		props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1_000);
		//The maximum number of unacknowledged requests the client will send on a single connection before blocking.
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 2);
		//Keep the acks value as default
		props.put(ProducerConfig.ACKS_CONFIG, "all");

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);

	}
}
