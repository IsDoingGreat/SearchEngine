package in.nimbo.isDoing.searchEngine.elastic;

import in.nimbo.isDoing.searchEngine.interfaces.searchDAO;
import org.elasticsearch.action.index.IndexRequest;

import java.util.Date;

public class ElasticSearchDAOImpl implements searchDAO {

    @Override
    public void insert(String title, String description, String text) {
        IndexRequest indexRequest = new IndexRequest("posts", "_doc")
                .source("title", text,
                        "desc", description,
                        "text", text);

//Run Index Query Sync   //client.indexAsync for Async
        try {
            ElasticClient.getClient().index(indexRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
