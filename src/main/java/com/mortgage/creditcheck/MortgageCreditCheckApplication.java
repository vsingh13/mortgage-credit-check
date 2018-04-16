package com.mortgage.creditcheck;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.mortgage.creditcheck.service.DocIngestTopicStreamService;
import com.mortgage.creditcheck.service.ConsumeStreamedTopic;

/**
 * Mortgage Credit check application will be called to push the document into
 * kafka topic, so that event can be handled from that topic by the streaming service
 * 
 * @author Vikas Singh
 *
 */
@SpringBootApplication
public class MortgageCreditCheckApplication {

	public static void main(String[] args) {
		SpringApplication.run(MortgageCreditCheckApplication.class, args);
		DocIngestTopicStreamService docIngestTopicStreamService = new DocIngestTopicStreamService();
		docIngestTopicStreamService.processStreamData();
		ConsumeStreamedTopic consumeStreamedTopic = new ConsumeStreamedTopic();
		consumeStreamedTopic.processStreamedTopic();
	}
}
