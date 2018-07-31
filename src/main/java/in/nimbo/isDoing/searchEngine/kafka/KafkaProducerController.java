package in.nimbo.isDoing.searchEngine.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static in.nimbo.isDoing.searchEngine.kafka.KafkaImpl.*;

public class KafkaProducerController {
    private static Logger logger = LoggerFactory.getLogger(KafkaProducerController.class);
    private Producer<Long, String> producer;

    public KafkaProducerController() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    public void produce(long index, String text) {
        ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC_NAME, index, text);
        try {
            producer.send(record).get();
        } catch (Exception e) {
            logger.warn("error in sending to kafka", e);
            e.printStackTrace();
        }
    }
}
