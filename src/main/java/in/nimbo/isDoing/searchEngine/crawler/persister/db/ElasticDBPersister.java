package in.nimbo.isDoing.searchEngine.crawler.persister.db;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This Class is not Thread-Safe!
 */
public class ElasticDBPersister implements DBPersister {
    private static final Logger logger = LoggerFactory.getLogger(ElasticDBPersister.class);
    private static final String DEFAULT_FLUSH_SIZE = "2";
    private static final String DEFAULT_FLUSH_NUMBER = "100";

    private BulkRequest elasticBulkRequest;
    private RestHighLevelClient client;

    private String elasticIndex;
    private String elasticDocument;
    private int elasticFlushSizeLimit;
    private int elasticFlushNumberLimit;

    public ElasticDBPersister() {
        Engine.getOutput().show("Creating ElasticDBPersister...");
        logger.info("Creating ElasticDBPersister...");

        client = ElasticClient.getClient();
        elasticBulkRequest = new BulkRequest();
        elasticIndex = Engine.getConfigs().get("crawler.persister.db.elastic.index");
        elasticDocument = Engine.getConfigs().get("crawler.persister.db.elastic.document");

        elasticFlushSizeLimit = Integer.parseInt(Engine.getConfigs().get(
                "crawler.persister.db.elastic.flushSizeLimit", DEFAULT_FLUSH_SIZE));

        elasticFlushNumberLimit = Integer.parseInt(Engine.getConfigs().get(
                "crawler.persister.db.elastic.flushNumberLimit", DEFAULT_FLUSH_NUMBER));

        logger.info("ElasticDBPersister Created With Settings");
    }

    @Override
    public void persist(Page page) throws Exception {
        String text = null;
        try {
            text = page.getExtractedText();
        } catch (Exception ignored) {
        }

        IndexRequest indexRequest = new IndexRequest(elasticIndex, elasticDocument);
        indexRequest.source(
                "title", page.getTitle(),
                "url", page.getUrl().toExternalForm(),
                "text", text == null ? page.getText() : text
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
        if (elasticBulkRequest.numberOfActions() > 0)
            client.bulk(elasticBulkRequest);

        elasticBulkRequest = new BulkRequest();
    }
}
