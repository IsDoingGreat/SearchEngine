package in.nimbo.isDoing.searchEngine.crawler.urlqueue;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.kafka.KafkaConsumerController;
import in.nimbo.isDoing.searchEngine.kafka.KafkaProducerController;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
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
    private boolean manualPartitionAssignment;

    public KafkaUrlQueue() {
        logger.info("Creating Kafka URLQueue...");
        Engine.getOutput().show("Creating Kafka URLQueue...");

        // TODO: 8/5/18 Make Defaults Constant
        topicName = Engine.getConfigs().get("crawler.urlQueue.kafka.topicName", "urls");
        brokers = Engine.getConfigs().get("kafka.brokers");
        consumerGroupId = Engine.getConfigs().get("crawler.urlQueue.kafka.consumerGroupId", "1");
        consumerMaxPoolRecords = Integer.parseInt(Engine.getConfigs().get("crawler.urlQueue.kafka.consumerMaxPoolRecords", "30"));
        producerClientId = Engine.getConfigs().get("crawler.urlQueue.kafka.producerClientId", "Crawler Kafka UrlQueue");
        manualPartitionAssignment = Boolean.parseBoolean(Engine.getConfigs().get("crawler.urlQueue.kafka.manualPartitionAssignment"));
        if (manualPartitionAssignment) {
            int partition = Integer.parseInt(Engine.getConfigs().get("crawler.urlQueue.kafka.partition"));
            consumerController = new KafkaConsumerController(brokers, consumerGroupId, consumerMaxPoolRecords, topicName, partition);
        } else {
            consumerController = new KafkaConsumerController(brokers, consumerGroupId, consumerMaxPoolRecords, topicName);
        }
        producerController = new KafkaProducerController(brokers, producerClientId, topicName, "0");
    }


    @Override
    public void push(String url) {
        try {
            producerController.produce(new URL(url).getHost(), url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> pop(int number) {
        Iterable<ConsumerRecord<String, String>> records = consumerController.get();
        List<String> list = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            list.add(record.value());
        }
        return Collections.unmodifiableList(list);
    }

    @Override
    public void stop() {
        Engine.getOutput().show("Stopping Kafka consumerController... ");
        consumerController.stop();

        Engine.getOutput().show("Stopping Kafka producerController... ");
        producerController.stop();
    }


    public Status getStatus() {
        Status status = new Status("KafkaUrlQueue", "");
        KafkaConsumer<String, String> consumer = consumerController.getConsumer();
        status.addLine("Topic List: ");
        try {
            consumer.listTopics().forEach((s, partitionInfos) ->
            {
                status.addLine("\t" + s + " : " + partitionInfos);
            });
        } catch (Exception e) {
            status.addLine(e.getMessage());
        }

        status.addLine("TopicName: " + topicName);
        status.addLine("consumerGroupId: " + consumerGroupId);
        status.addLine("consumer Assignments: ");

        try {
            for (TopicPartition partition : consumer.assignment()) {
                status.addLine("\t" + partition.toString());
                status.addLine("\tposition in this partition : " + consumer.position(partition));
                status.addLine("\tstart position in this partition : " + consumer.beginningOffsets(Collections.singleton(partition)));
                status.addLine("\tend position in this partition : " + consumer.endOffsets(Collections.singleton(partition)));
            }
        } catch (Exception e) {
            status.addLine(e.getMessage());
        }
        return status;
    }
}
