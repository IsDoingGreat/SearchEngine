package in.nimbo.isDoing.searchEngine.newsReader.persister.db;

import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.newsReader.controller.JmxCounter;
import in.nimbo.isDoing.searchEngine.newsReader.model.Item;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This Class is not Thread-Safe!
 */
public class ElasticItemPersister implements DBPersister {
    private static final Logger logger = LoggerFactory.getLogger(ElasticItemPersister.class);
    private static final String DEFAULT_FLUSH_SIZE = "2";
    private static final String DEFAULT_FLUSH_NUMBER = "150";

    private BulkRequest elasticBulkRequest;
    private RestHighLevelClient client;

    private String elasticIndex;
    private String elasticDocument;
    private int elasticFlushSizeLimit;
    private int elasticFlushNumberLimit;


    public ElasticItemPersister() {
        Engine.getOutput().show("Creating ElasticItemPersister...");
        logger.info("Creating ElasticItemPersister...");

        client = ElasticClient.getClient();
        elasticBulkRequest = new BulkRequest();
        elasticIndex = Engine.getConfigs().get("newsReader.persister.db.elastic.index");
        elasticDocument = Engine.getConfigs().get("newsReader.persister.db.elastic.document");

        elasticFlushSizeLimit = Integer.parseInt(Engine.getConfigs().get(
                "newsReader.persister.db.elastic.flushSizeLimit", DEFAULT_FLUSH_SIZE));

        elasticFlushNumberLimit = Integer.parseInt(Engine.getConfigs().get(
                "newsReader.persister.db.elastic.flushNumberLimit", DEFAULT_FLUSH_NUMBER));

        logger.info("ElasticItemPersister Created With Settings");
    }

    @Override
    public void persist(Item item) throws Exception {
        IndexRequest indexRequest = new IndexRequest(elasticIndex, elasticDocument);
        indexRequest.source(
                "category", item.getChannel().getCategory(),
                "title", item.getTitle(),
                "url", item.getLink().toExternalForm(),
                "text", item.getText(),
                "date", item.getDate()
        );

        elasticBulkRequest.add(indexRequest);

        flushIfNeeded();
    }


    private void flushIfNeeded() throws Exception {
        if (elasticBulkRequest.estimatedSizeInBytes() / 1000_000 >= elasticFlushSizeLimit ||
                elasticBulkRequest.numberOfActions() >= elasticFlushNumberLimit) {
            flush();
        }
    }

    @Override
    public void flush() throws Exception {
        if (elasticBulkRequest.numberOfActions() > 0) {
            client.bulk(elasticBulkRequest);
            JmxCounter.incrementSuccessfulItemsOfElasticPersister(elasticBulkRequest.numberOfActions());
        }

        elasticBulkRequest = new BulkRequest();

    }
}
