package in.nimbo.isDoing.searchEngine.crawler.page;

import java.net.URL;
import java.util.Set;

public interface Page {
    void parse();

    String getBody();

    URL getUrl();

    String getExtractedText() throws Exception;

    String getText();

    String getTitle();

    String getDescription();

    Set<String> getOutgoingUrls();

    String getLang();
}
