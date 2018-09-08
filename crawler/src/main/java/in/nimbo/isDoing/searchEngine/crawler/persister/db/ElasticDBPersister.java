package in.nimbo.isDoing.searchEngine.crawler.persister.db;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.crawler.persister.PagePersisterImpl;
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

    private BulkRequest elasticBulkRequest;
    private RestHighLevelClient client;

    private String elasticIndex;
    private String elasticDocument;
    private PagePersisterImpl pagePersister;

    public ElasticDBPersister(PagePersisterImpl pagePersister) {

        Engine.getOutput().show("Creating ElasticItemPersister...");
        this.pagePersister = pagePersister;

        logger.info("Creating ElasticItemPersister...");

        client = ElasticClient.getClient();
        elasticBulkRequest = new BulkRequest();
        elasticIndex = Engine.getConfigs().get("crawler.persister.db.elastic.index");
        elasticDocument = Engine.getConfigs().get("crawler.persister.db.elastic.document");


        logger.info("ElasticItemPersister Created With Settings");
    }

    @Override
    public void persist(Page page) throws Exception {
        IndexRequest indexRequest = new IndexRequest(elasticIndex, elasticDocument);
        indexRequest.source(
                "title", page.getTitle(),
                "url", page.getUrl().toExternalForm(),
                "text", page.getText()
        );

        elasticBulkRequest.add(indexRequest);

        flushIfNeeded();
    }


    private void flushIfNeeded() throws Exception {
        if (elasticBulkRequest.estimatedSizeInBytes() / 1000_000 >= pagePersister.getElasticFlushSizeLimit() ||
                elasticBulkRequest.numberOfActions() >= pagePersister.getElasticFlushNumberLimit()) {
            flush();
        }
    }

    @Override
    public void flush() throws Exception {
        if (elasticBulkRequest.numberOfActions() > 0) {
            client.bulk(elasticBulkRequest);
        }

        elasticBulkRequest = new BulkRequest();
    }
}
