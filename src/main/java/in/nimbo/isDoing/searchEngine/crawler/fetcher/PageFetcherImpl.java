package in.nimbo.isDoing.searchEngine.crawler.fetcher;

import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.crawler.page.WebPage;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;

public class PageFetcherImpl implements PageFetcher {
    private final static Logger logger = LoggerFactory.getLogger(PageFetcherImpl.class);
    private static final String TIMEOUT = "3000";
    private static final String FOLLOW_REDIRECTS = "false";
    private static final String USER_AGENT = null;
    private int timeout;
    private boolean followRedirects;
    private String userAgent;

    public PageFetcherImpl() throws IOException {
        timeout = Integer.parseInt(Engine.getConfigs().get("crawler.pageFetcher.timeout", TIMEOUT));
        followRedirects = Boolean.parseBoolean(Engine.getConfigs().get("crawler.pageFetcher.followRedirects", FOLLOW_REDIRECTS));
        userAgent = Engine.getConfigs().get("crawler.pageFetcher.userAgent", USER_AGENT);
        logger.info("PageFetcher Created With Settings:\n" +
                "\ttimeout= " + timeout + "\n" +
                "\tfollowRedirects= " + followRedirects + "\n" +
                "\tuserAgent= " + userAgent + "\n");

        List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
    }

    @Override
    public Page fetch(URL url) throws IOException {
        Connection connection = Jsoup.connect(url.toExternalForm()).validateTLSCertificates(false).followRedirects(followRedirects).timeout(this.timeout);
        if (userAgent != null)
            connection.userAgent(userAgent);

        //Jsoup Handle Content Type Automatically
        Connection.Response response = connection.execute();

        if (response.statusCode() != 200)
            throw new BadStatusCodeException(response.statusCode() + ":" + response.statusCode() + " for site " + url.toExternalForm());


        if (!response.contentType().contains("html"))
            throw new NotSupportedContentTypeException(response.contentType() + "Not Supported");



        return new WebPage(response.body(), url);
    }

    @Override
    public void stop() {

    }
}