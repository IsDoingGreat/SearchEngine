package in.nimbo.isDoing.searchEngine.news_reader;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.news_reader.dao.ChannelDAO;
import in.nimbo.isDoing.searchEngine.news_reader.dao.ItemDAO;
import in.nimbo.isDoing.searchEngine.news_reader.impl.HBaseChannelDAO;
import in.nimbo.isDoing.searchEngine.news_reader.impl.ItemDAOImpl;
import in.nimbo.isDoing.searchEngine.news_reader.model.Channel;
import in.nimbo.isDoing.searchEngine.news_reader.model.Item;
import in.nimbo.isDoing.searchEngine.news_reader.persister.Persister;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

public class NewsReaderService implements Service {
    private static volatile NewsReaderService instance;
    final Logger logger = LoggerFactory.getLogger(NewsReaderService.class);
    private BlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    private ChannelDAO channelDAO = new HBaseChannelDAO();
    private ItemDAO itemDAO = new ItemDAOImpl(queue);
    private Persister persister = new Persister(queue);
    private boolean started = false;

    public static void main(String[] args) throws Exception {
        Engine.start(new ConsoleOutput());
        new NewsReaderService().start();
    }

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10, r -> {
        Thread thread = Executors.defaultThreadFactory().newThread(r);
        thread.setDaemon(true);
        return thread;
    });


    public void setChannelDAO(ChannelDAO channelDAO) {
        this.channelDAO = channelDAO;
    }

    public void crawl(URL rssLink) {
        SiteCrawler siteCrawler = new SiteCrawler(rssLink, channelDAO, itemDAO);
        executorService.schedule(siteCrawler, 0, TimeUnit.NANOSECONDS);
        logger.debug("{} scheduled in executor service!", rssLink);
    }

    @Override
    public synchronized void start() {
        if (started)
            throw new IllegalStateException("Already Started");


        started = true;
        executorService.scheduleAtFixedRate(new UpdaterThread(), 0, 30, TimeUnit.SECONDS);
        executorService.schedule(persister, 0, TimeUnit.SECONDS);
    }


    @Override
    public Status status() {
        return null;
    }

    @Override
    public String getName() {
        return "newsReader";
    }

    @Override
    public void stop() {
        executorService.shutdownNow();
        try {
            Engine.getOutput().show("Waiting Five Second To Stop Threads....");
            executorService.awaitTermination(5, TimeUnit.SECONDS);
            channelDAO.stop();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class UpdaterThread implements Runnable {
        final Logger logger = LoggerFactory.getLogger(UpdaterThread.class);

        @Override
        public void run() {
            try {
                List<Channel> channels = channelDAO.getChannelsUpdatedBefore(2);
                for (Channel channel : channels) {
                    logger.info("Scheduled Channel Crawling Started for {}", channel.getName());
                    channel.setLastUpdate(new Date().getTime());
                    channelDAO.updateChannelLastDate(channel);
                    crawl(channel.getRssLink());
                }

            } catch (Exception e) {
                logger.error("error in Channels Crawler Starter Thread", e);
            }
        }

    }

}
