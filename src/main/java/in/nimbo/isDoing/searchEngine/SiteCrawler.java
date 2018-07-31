package in.nimbo.isDoing.searchEngine;

import de.l3s.boilerpipe.extractors.ArticleExtractor;
import in.nimbo.isDoing.searchEngine.interfaces.UrlQueue;
import in.nimbo.isDoing.searchEngine.interfaces.searchDAO;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class SiteCrawler implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    private URL url;
    private UrlQueue<String> queue;
    private searchDAO dao;
    private LRU lru;

    public SiteCrawler(URL url, UrlQueue<String> queue, searchDAO dao, LRU lru) {
        this.url = url;
        this.queue = queue;
        this.dao = dao;
        this.lru = lru;
    }

    @Override
    public void run() {
        try {
            if (!lru.get(url.getHost())) {
                return;// TODO: 7/30/18 Push AGain THE Site
            } else {
                lru.add(url.getHost());
            }

            Document doc = Jsoup.connect(url.toExternalForm()).validateTLSCertificates(false).get();


            String text = ArticleExtractor.INSTANCE.getText(doc.outerHtml());
            String title = getHeadElementText(doc, "title");

            System.out.println(title);
            String description = getHeadElementText(doc, "meta[name=description]");
            System.out.println(description);

            dao.insert(title, description, text);
            for (String s : getUrls(doc)) {
                queue.push(s);
            }

        } catch (Exception e) {
            logger.warn("error in siteCrawler", e);
            e.printStackTrace();
        }
    }

    public boolean isEn(Document doc) {
        Element html = doc.selectFirst("html");
        if (html != null)
            return html.attr("lang").equals("en") || html.attr("lang").startsWith("en-");
        else
            return false;
    }

    Set<String> getUrls(Document document) {
        Set<String> urls = new HashSet<>();
        Elements links = document.select("a[href]");
        for (Element link : links)
            urls.add(link.attr("abs:href"));

        return urls;
    }

    public String getHeadElementText(Document doc, String selector) {
        try {
            Elements titleElement = doc.head().select(selector);
            if (titleElement != null) {
                return titleElement.first().text();
            }
            return "";
        } catch (Exception e) {
            return "";
        }
    }
}
