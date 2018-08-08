package in.nimbo.isDoing.searchEngine.crawler.urlqueue;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.kafka.KafkaConsumerController;
import in.nimbo.isDoing.searchEngine.kafka.KafkaProducerController;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KafkaUrlQueue implements URLQueue {
    private final static Logger logger = LoggerFactory.getLogger(KafkaUrlQueue.class);
    private KafkaConsumerController consumerController;
    private KafkaProducerController producerController;
    private String topicName;
    private String brokers;
    private String consumerGroupId;
    private int consumerMaxPoolRecords;
    private String producerClientId;

    public KafkaUrlQueue() {
        logger.info("Creating Kafka URLQueue...");
        Engine.getOutput().show("Creating Kafka URLQueue...");

        // TODO: 8/5/18 Make Defaults Constant
        topicName = Engine.getConfigs().get("crawler.urlQueue.kafka.topicName", "urls");
        brokers = Engine.getConfigs().get("crawler.urlQueue.kafka.brokers");
        consumerGroupId = Engine.getConfigs().get("crawler.urlQueue.kafka.consumerGroupId", "1");
        consumerMaxPoolRecords = Integer.parseInt(Engine.getConfigs().get("crawler.urlQueue.kafka.consumerMaxPoolRecords", "30"));
        producerClientId = Engine.getConfigs().get("crawler.urlQueue.kafka.producerClientId", "Crawler Kafka UrlQueue");

        consumerController = new KafkaConsumerController(brokers, consumerGroupId, consumerMaxPoolRecords, topicName);
        producerController = new KafkaProducerController(brokers, producerClientId, topicName);
    }


    @Override
    public void push(String url) {
        try {
            producerController.produce(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> pop(int number) {
        Iterable<ConsumerRecord<Long, String>> records = consumerController.get();
        List<String> list = new ArrayList<>();
        for (ConsumerRecord<Long, String> record : records) {
            list.add(record.value());
        }
        return Collections.unmodifiableList(list);
    }

    @Override
    public void stop() {
        consumerController.stop();
        producerController.stop();
    }
}
