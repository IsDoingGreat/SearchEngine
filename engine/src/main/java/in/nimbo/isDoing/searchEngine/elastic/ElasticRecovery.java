package in.nimbo.isDoing.searchEngine.elastic;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.kafka.KafkaProducerController;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ElasticRecovery {
    private RestHighLevelClient client;
    private String index = "posts";
    private KafkaProducerController producer;

    public ElasticRecovery() {
        client = ElasticClient.getClient();
        String brokers = Engine.getConfigs().get("crawler.urlQueue.kafka.brokers");
        producer = new KafkaProducerController(brokers, "recoveryPr", "cleanUrls");
    }


    public static void main(String[] args) throws Exception {
        Engine.start(new ConsoleOutput());
        new ElasticRecovery().scroll();
    }

    public void scroll() throws IOException, ExecutionException, InterruptedException {
        long time = System.currentTimeMillis();
        int total = 0;
        int size = 650;
        final Scroll scroll = new Scroll(TimeValue.timeValueSeconds(30L));
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(size);
        searchSourceBuilder.sort("_doc");
        searchSourceBuilder.fetchSource("url", null);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        long counter = 0;
        while (searchHits != null && searchHits.length > 0) {
            counter++;
            if (counter % 20 == 0)
                System.out.println("total: " + total + "  time:" + ((System.currentTimeMillis() - time) / 1000) + "s");

            if (counter % 150 == 0)
                Thread.sleep(3000);

            total += searchHits.length;
            for (SearchHit hit : searchHits) {
                producer.produce((String) hit.getSourceAsMap().get("url"));
            }
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.searchScroll(scrollRequest);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        System.out.println(total);
        System.out.println(System.currentTimeMillis() - time);
        System.exit(0);
    }
}
