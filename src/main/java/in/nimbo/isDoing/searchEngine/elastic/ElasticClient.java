package in.nimbo.isDoing.searchEngine.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticClient {

    private static volatile ElasticClient instance = new ElasticClient();
    private RestHighLevelClient client;

    private ElasticClient() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.122.121", 9200, "http")));
    }

    public static ElasticClient getInstance() {
        return instance;
    }

    public static RestHighLevelClient getClient() {
        return getInstance().client;
    }
}
