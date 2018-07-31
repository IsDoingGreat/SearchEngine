package in.nimbo.isDoing.searchEngine.kafka;

import in.nimbo.isDoing.searchEngine.interfaces.UrlQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KafkaUrlQueue implements UrlQueue<String> {
    private KafkaConsumerController consumerController;
    private KafkaProducerController producerController;

    public KafkaUrlQueue() {
        consumerController = new KafkaConsumerController();
        producerController = new KafkaProducerController();
    }

    @Override
    public void push(String url) {
        producerController.produce(1, url);
    }

    @Override
    public List<String> pop(int number) {
        Iterable<ConsumerRecord<Long, String>> records = consumerController.get(50);
        List<String> list = new ArrayList<>();
        for (ConsumerRecord<Long, String> record : records) {
            list.add(record.value());
        }
        return Collections.unmodifiableList(list);
    }
}
