package in.nimbo.isDoing.searchEngine.crawler;

import in.nimbo.isDoing.searchEngine.crawler.interfaces.Page;
import in.nimbo.isDoing.searchEngine.crawler.interfaces.PagePersister;
import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import org.elasticsearch.action.index.IndexRequest;

import java.io.IOException;

public class ElasticSearchPagePersister implements PagePersister {

    @Override
    public void insert(Page page) throws Exception {
        String text = page.getExtractedText();
        IndexRequest indexRequest = new IndexRequest("posts", "_doc")
                .source("title", page.getTitle(),
                        "desc", page.getDescription(),
                        "text", text.isEmpty() ? page.getText() : text,
                        "link", page.getUrl().toExternalForm());

        ElasticClient.getClient().index(indexRequest);
    }

    @Override
    public void stop() {
        try {
            ElasticClient.getClient().close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
