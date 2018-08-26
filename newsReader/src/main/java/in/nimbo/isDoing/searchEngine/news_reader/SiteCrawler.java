package in.nimbo.isDoing.searchEngine.news_reader;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import in.nimbo.isDoing.searchEngine.news_reader.dao.ChannelDAO;
import in.nimbo.isDoing.searchEngine.news_reader.dao.ItemDAO;
import in.nimbo.isDoing.searchEngine.news_reader.model.Channel;
import in.nimbo.isDoing.searchEngine.news_reader.model.Item;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Date;

public class SiteCrawler implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(SiteCrawler.class);
    private final int TIMEOUT = 3000;
    private ChannelDAO channelDAO;
    private ItemDAO itemDAO;
    private URL urlAddress;

    public SiteCrawler(URL urlAddress, ChannelDAO channelDAO, ItemDAO itemDAO) {
        this.urlAddress = urlAddress;
        this.channelDAO = channelDAO;
        this.itemDAO = itemDAO;
    }

    public void update() {
        logger.info("Start Updating: {}", urlAddress);
        try {
            SyndFeed feed = getSyndFeed();

            logger.info("feed reading was successful for site : {}," +
                    "  {} items found!", feed.getTitle(), feed.getEntries().size());

            logger.trace(feed.toString());
            Channel channel;

            channel = channelDAO.getChannel(urlAddress);
            if (channel == null) {
                channel = new Channel(feed.getTitle(), urlAddress, new Date().getTime());
                channelDAO.insertChannel(channel);
            }
            channelDAO.updateChannelLastDate(channel);


            for (SyndEntry entry : feed.getEntries()) {
                String description = entry.getDescription() != null ? entry.getDescription().getValue() : "";

                Item item = new Item(entry.getTitle(), new URL(entry.getLink()), description, entry.getPublishedDate(), channel);

                logger.debug("Checking item {}", item.getTitle());
                if (itemDAO.checkItemExists(item)) continue;

                try {

                    String newsText = extractTextAutomatically(item.getLink());

                    item.setText(newsText);
                    itemDAO.insertItem(item);
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                } catch (Exception e) {
                    logger.debug("failed to load fullText for item   {}, for more information enable debug level", item.getTitle());
                    logger.debug("error", e);
                    logger.trace("Printing Item : {}", item);
                }

            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            System.err.println("crawling failed for site" + urlAddress + "! for more information see rssReader.log");
        }

    }

    SyndFeed getSyndFeed() throws FeedException, IOException {
        SyndFeedInput input = new SyndFeedInput();
        return input.build(new XmlReader(urlAddress));
    }

    /**
     * this method extract article text using given patterns.
     * Notice: If there is no pattern or config for this link available, this method is useless.
     * instead look at see also part.
     *
     * @param link        article link
     * @param bodyPattern html selector for body of article e.g "div#article"
     * @param adPatterns  patterns for advertisements to be removed from article text
     * @return extracted article text
     * @throws IOException                                      if url is not valid
     * @throws org.jsoup.select.Selector.SelectorParseException if one of passed patterns is not valid (unchecked exception)
     * @throws IllegalStateException                            if element not found or text is null
     */
    String extractTextByPattern(URL link, String bodyPattern, String[] adPatterns) throws IOException {
        Document doc = fetchSite(link).parse();

        for (String adPattern : adPatterns)
            doc.select(adPattern).remove();

        Element firstElement = doc.select(bodyPattern).first();

        if (firstElement == null)
            throw new IllegalStateException("element not found");

        String text = firstElement.text();
        if (text.trim().isEmpty())
            throw new IllegalStateException("text is null");

        return text;
    }

    String extractTextAutomatically(URL link) throws BoilerpipeProcessingException, IOException {
        Connection.Response response = fetchSite(link);
        return ArticleExtractor.INSTANCE.getText(response.body());
    }

    Connection.Response fetchSite(URL link) throws IOException {
        return Jsoup.connect(link.toExternalForm()).method(Connection.Method.GET).timeout(TIMEOUT).execute();
    }

    @Override
    public void run() {
        update();
    }

}