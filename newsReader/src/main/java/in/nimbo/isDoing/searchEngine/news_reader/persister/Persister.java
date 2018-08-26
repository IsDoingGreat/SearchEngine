package in.nimbo.isDoing.searchEngine.news_reader.persister;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.kafka.KafkaProducerController;
import in.nimbo.isDoing.searchEngine.news_reader.model.Item;
import in.nimbo.isDoing.searchEngine.news_reader.persister.db.ElasticItemPersister;
import in.nimbo.isDoing.searchEngine.news_reader.persister.db.HBaseItemPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class Persister implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(Persister.class);
    private String topicName;
    private String brokers;
    private String producerClientId;
    private BlockingQueue<Item> queue;
    private KafkaProducerController producer;
    private ElasticItemPersister elasticItemPersister;
    private HBaseItemPersister hBaseItemPersister;

    public Persister(BlockingQueue<Item> queue) {


        logger.info("Creating Item Persister...");
        Engine.getOutput().show("Creating Persister...");

        topicName = Engine.getConfigs().get("newsReader.persister.kafka.topicName");
        brokers = Engine.getConfigs().get("newsReader.persister.kafka.brokers");
        producerClientId = Engine.getConfigs().get("newsReader.persister.in.nimbo.isDoing.searchEngine.kafka.producerClientId", "NewsReader Kafka UrlQueue");
        producer = new KafkaProducerController(brokers, producerClientId, topicName);


        this.queue = queue;
        elasticItemPersister = new ElasticItemPersister();
        hBaseItemPersister = new HBaseItemPersister();
    }

    @Override
    public void run() {
        try {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Item item = queue.take();
                    producer.produce(item.getLink().toExternalForm(), item.getText());
                    hBaseItemPersister.persist(item);
                    elasticItemPersister.persist(item);
                }
            } catch (InterruptedException e) {
                logger.info(Thread.currentThread() + "Interrupted... ");
            }

            //Trying to free Blocking Queue...
            Item item;
            while ((item = queue.poll()) != null) {
                elasticItemPersister.persist(item);
                hBaseItemPersister.persist(item);
                if (queue.isEmpty()) {
                    elasticItemPersister.flush();
                    hBaseItemPersister.flush();
                }
            }

            elasticItemPersister.flush();
            hBaseItemPersister.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
