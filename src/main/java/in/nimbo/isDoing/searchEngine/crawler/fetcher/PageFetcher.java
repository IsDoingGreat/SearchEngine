package in.nimbo.isDoing.searchEngine.crawler.fetcher;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;

import java.net.URL;

public interface PageFetcher {
    Page fetch(URL url) throws Exception;

    void stop();
    class BadStatusCodeException extends RuntimeException {
        public BadStatusCodeException(String message) {
            super(message);
        }
    }

    class NotSupportedContentTypeException extends RuntimeException {
        public NotSupportedContentTypeException(String message) {
            super(message);
        }
    }
}
