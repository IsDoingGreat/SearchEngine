package in.nimbo.isDoing.searchEngine.crawler.interfaces;

import java.io.IOException;
import java.net.URL;

@FunctionalInterface
public interface PageFetcher {
    Page fetch(URL url) throws IOException;

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
