package in.nimbo.isDoing.searchEngine.elastic;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class ElasticClient {

    private static volatile ElasticClient instance = new ElasticClient();
    private RestHighLevelClient client;

    private ElasticClient() {
        String hostString = Engine.getConfigs().get("elastic.hosts");
        String[] hosts = hostString.split(";");
        if (hosts.length == 0)
            throw new IllegalStateException("Elastic Host Not Defined");

        HttpHost[] clients = new HttpHost[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            String[] clientData = hosts[i].split("\\|");
            if (clientData.length != 3)
                throw new IllegalStateException("host is bad formed");

            clients[i] = new HttpHost(clientData[0], Integer.parseInt(clientData[1]), clientData[2]);
        }

        client = new RestHighLevelClient(
                RestClient.builder(clients));
    }

    public static ElasticClient getInstance() {
        return instance;
    }

    public static RestHighLevelClient getClient() {
        return getInstance().client;
    }

    public static void close() throws IOException {
        getClient().close();
    }
}
