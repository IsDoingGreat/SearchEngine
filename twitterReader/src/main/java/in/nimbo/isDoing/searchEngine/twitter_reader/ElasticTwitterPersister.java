package in.nimbo.isDoing.searchEngine.twitter_reader;

import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

import java.io.IOException;

public class ElasticTwitterPersister {
    static class ElasticTwitterPersisterException extends  Exception {
        ElasticTwitterPersisterException(Throwable cause) {
            super(cause);
        }

        public ElasticTwitterPersisterException(String message) {
            super(message);
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(ElasticTwitterPersister.class);

    private BulkRequest elasticBulkRequest;
    private RestHighLevelClient client;

    private String elasticIndex;
    private String elasticDocument;
    private int bulkSize;

    ElasticTwitterPersister() {
        Engine.getOutput().show("Creating ElasticTwitterPersister");

        logger.info("Creating ElasticTwitterPersister");

        client = ElasticClient.getClient();
        elasticBulkRequest = new BulkRequest();
        elasticIndex = Engine.getConfigs().get("elastic.twitter.persister");
        elasticDocument = Engine.getConfigs().get("elastic.twitter.document");
        bulkSize = Integer.parseInt(Engine.getConfigs().get("elastic.twitter.bulksize"));

        logger.info("ElasticTwitterPersister created and configs are set");
    }

    void persist(Status status) throws ElasticTwitterPersisterException{
        try {
            IndexRequest indexRequest = new IndexRequest(elasticIndex, elasticDocument);
            indexRequest.source(
                    "id", status.getId(),
                    "url", status.getURLEntities(),
                    "location", status.getGeoLocation(),
                    "date", status.getCreatedAt(),
                    "text", status.getText()
            );

            elasticBulkRequest.add(indexRequest);

            flushIfNeeded();
        } catch (Exception e) {
            throw new ElasticTwitterPersisterException(e);
        }
    }

    private void flushIfNeeded() throws IOException {
        if (elasticBulkRequest.numberOfActions() >= bulkSize)
            flush();
    }

    void flush() throws IOException {
        if (elasticBulkRequest.numberOfActions() > 0)
            client.bulk(elasticBulkRequest);

        elasticBulkRequest = new BulkRequest();

    }
}