package in.nimbo.isDoing.searchEngine.crawler.page;

import java.net.URL;
import java.util.Map;

public interface Page {
    void parse();

    String getBody();

    URL getUrl();

    String getExtractedText() throws Exception;

    String getText();

    String getTitle();

    String getDescription();

    Map<String, String> getOutgoingUrls();

    String getLang();
}
