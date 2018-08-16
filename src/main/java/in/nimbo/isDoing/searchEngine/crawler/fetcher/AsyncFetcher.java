package in.nimbo.isDoing.searchEngine.crawler.fetcher;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.crawler.page.WebPage;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class AsyncFetcher implements PageFetcher {
    private final static Logger logger = LoggerFactory.getLogger(PageFetcherImpl.class);
    private static final String TIMEOUT = "3000";
    private static final String FOLLOW_REDIRECTS = "false";
    private static final String USER_AGENT = null;
    private int timeout;
    private boolean followRedirects;
    private String userAgent;
    private AsyncHttpClient asyncHttpClient;

    public AsyncFetcher() {
        timeout = Integer.parseInt(Engine.getConfigs().get("crawler.pageFetcher.timeout", TIMEOUT));
        followRedirects = Boolean.parseBoolean(Engine.getConfigs().get("crawler.pageFetcher.followRedirects", FOLLOW_REDIRECTS));
        userAgent = Engine.getConfigs().get("crawler.pageFetcher.userAgent", USER_AGENT);
        logger.info("PageFetcher Created With Settings:\n" +
                "\ttimeout= " + timeout + "\n" +
                "\tfollowRedirects= " + followRedirects + "\n" +
                "\tuserAgent= " + userAgent + "\n");
        DefaultAsyncHttpClientConfig.Builder builder = Dsl.config().setRequestTimeout(timeout).setFollowRedirect(followRedirects);
        if (userAgent != null)
            builder.setUserAgent(userAgent);

        asyncHttpClient = asyncHttpClient(builder);
    }

    @Override
    public Page fetch(URL url) throws Exception {
        Response response = asyncHttpClient.prepareGet(url.toExternalForm()).execute().get();

        if (response.getStatusCode() != 200)
            throw new BadStatusCodeException(response.getStatusCode() + ": for site " + url.toExternalForm());


        if (!response.getContentType().contains("html"))
            throw new NotSupportedContentTypeException(response.getContentType() + "Not Supported");


        return new WebPage(response.getResponseBody(), url);
    }

    @Override
    public void stop() {
        try {
            asyncHttpClient.close();
        } catch (IOException e) {
            logger.error("Fetcher close Error",e);
            Engine.getOutput().show(Output.Type.ERROR,"Error Stopping Fetcher");
        }
    }
}
