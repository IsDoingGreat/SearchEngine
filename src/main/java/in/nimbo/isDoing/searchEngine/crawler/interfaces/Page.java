package in.nimbo.isDoing.searchEngine.crawler.interfaces;

import java.net.URL;
import java.util.Map;
import java.util.Set;

public interface Page {
    void parse();

    String getBody();

    Map<String, String> getHeaders();

    URL getUrl();

    String getExtractedText() throws Exception;

    String getText();

    String getTitle();

    String getDescription();

    Set<String> getOutgoingUrls();
}
