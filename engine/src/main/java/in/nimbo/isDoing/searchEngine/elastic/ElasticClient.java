package in.nimbo.isDoing.searchEngine.elastic;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.engine.interfaces.HaveStatus;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

public class ElasticClient implements HaveStatus {

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

    @Override
    public Status status() {
        Status status = new Status("Elastic DB", "");
        try {
            Response response = getClient().getLowLevelClient().performRequest("GET", "/_cluster/health");
            try (InputStream is = response.getEntity().getContent()) {
                Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    status.addLine(entry.getKey() + ": " + entry.getValue());
                }
            }
        } catch (IOException e) {
            status().addLine(e.getMessage());
        }

        return status;
    }

    public Map<?, ?> getJson() {
        try {
            Response response = getClient().getLowLevelClient().performRequest("GET", "/_cluster/health");
            try (InputStream is = response.getEntity().getContent()) {
                return XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
        } catch (IOException e) {
            return Collections.singletonMap("errors", Collections.singletonList(e.getMessage()));
        }
    }
}
