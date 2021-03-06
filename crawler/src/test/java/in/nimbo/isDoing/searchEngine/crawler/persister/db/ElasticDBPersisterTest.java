package in.nimbo.isDoing.searchEngine.crawler.persister.db;

import in.nimbo.isDoing.searchEngine.crawler.controller.Counter;
import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.crawler.page.WebPage;
import in.nimbo.isDoing.searchEngine.crawler.persister.PagePersisterImpl;
import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Configs;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ElasticDBPersisterTest {
/*
    private BulkRequest elasticBulkRequest = new BulkRequest();

    @Before
    public void Setup() throws Exception {
        Engine.start(new Output() {
            @Override
            public void show(String object) {
            }

            @Override
            public void show(Type type, String object) {
            }
        }, new Configs() {
            @Override
            public void load() throws Exception {

            }

            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public String get(String key, String value) {
                return null;
            }

            @Override
            public Path getLoadedPath() {
                return null;
            }

            @Override
            public Properties getMap() {
                return null;
            }
        });
    }

    @After
    public void Shutdown() {
        Engine.shutdown();
    }

    @Test
    public void persistWithManualFlush() throws Exception {
        SearchRequest searchRequest = new SearchRequest(Engine.getConfigs().get("crawler.persister.db.elastic.index", "1"));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        ElasticDBPersister elasticDBPersister = new ElasticDBPersister(new PagePersisterImpl(new Counter()));

        Page page = new WebPage("Example Domain " +
                "This domain is established to be used for illustrative examples in documents. You may use this domain in examples without prior coordination or asking for permission. " +
                "More information...", new URL("http://example.com/"));
        page.parse();
        elasticDBPersister.persist(page);
        elasticDBPersister.flush();


        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = ElasticClient.getClient().search(searchRequest);
        for (SearchHit hit : searchResponse.getHits()) {
            assertEquals("", hit.getSourceAsMap().get("title"));
            assertEquals("http://example.com/", hit.getSourceAsMap().get("url"));
            assertEquals("Example Domain " +
                    "This domain is established to be used for illustrative examples in documents. You may use this domain in examples without prior coordination or asking for permission. " +
                    "More information...", hit.getSourceAsMap().get("text"));
        }
    }*/

//    @Test
//    public void persistWithAutomaticFlush(){
//    }
}
