package in.nimbo.isDoing.searchEngine.crawler.page;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class WebPage implements Page {
    private final static Logger logger = LoggerFactory.getLogger(WebPage.class);
    private String body;
    private Document document;
    private URL url;
    private String text;
    private Map<String, String> outgoingUrls;
    private String title;

    public WebPage(String body, URL url) {
        this.body = body;
        this.url = url;
    }

    @Override
    public String toString() {
        parse();
        return "\nTitle=" + getTitle() + "\nDescription=" + getDescription() + "\n";
    }

    @Override
    public void parse() {
        if (document == null) {
            document = Jsoup.parse(body, url.toExternalForm());
        }
    }

    @Override
    public String getBody() {
        return body;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public String getExtractedText() throws BoilerpipeProcessingException {
        Objects.requireNonNull(document);
        return ArticleExtractor.INSTANCE.getText(document.outerHtml());
    }

    @Override
    public String getText() {
        if (text != null)
            return text;

        Objects.requireNonNull(document);
        Element body = document.body();
        if (body != null)
            return text = body.text();
        return text = "";
    }

    @Override
    public String getTitle() {
        if (title != null)
            return title;
        Objects.requireNonNull(document);
        return title = document.title();
    }

    @Override
    public String getDescription() {
        Objects.requireNonNull(document);
        Elements metaTags = document.getElementsByTag("meta");
        for (Element metaTag : metaTags) {
            String content = metaTag.attr("content");
            String name = metaTag.attr("name");

            if ("description".equals(name)) {
                return content;
            }
        }
        return null;
    }

    @Override
    public Map<String, String> getOutgoingUrls() {
        if (outgoingUrls != null)
            return outgoingUrls;

        Objects.requireNonNull(document);
        Map<String, String> urls = new HashMap<>();
        Elements links = document.select("a[href]");
        String externalForm = this.url.toExternalForm();
        for (Element link : links) {
            String url = link.attr("abs:href");

            if (!url.startsWith("http") || url.trim().isEmpty() || url.equals(externalForm))
                continue;

            urls.put(url, link.text());
        }

        return outgoingUrls = urls;
    }

    /**
     * @return language code
     * @throws LanguageDetector.LanguageNotDetected if language is not detected
     */
    @Override
    public String getLang() {
        return LanguageDetector.getInstance().detectLanguage(getText());
    }
}
