package in.nimbo.isDoing.searchEngine.newsReader;

import com.codahale.metrics.jmx.JmxReporter;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.SystemConfigs;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.newsReader.controller.JmxCounter;
import in.nimbo.isDoing.searchEngine.newsReader.dao.ChannelDAO;
import in.nimbo.isDoing.searchEngine.newsReader.dao.ItemDAO;
import in.nimbo.isDoing.searchEngine.newsReader.impl.HBaseChannelDAO;
import in.nimbo.isDoing.searchEngine.newsReader.impl.ItemDAOImpl;
import in.nimbo.isDoing.searchEngine.newsReader.model.Channel;
import in.nimbo.isDoing.searchEngine.newsReader.model.Item;
import in.nimbo.isDoing.searchEngine.newsReader.persister.Persister;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class NewsReaderService implements Service {
    private static volatile NewsReaderService instance;
    private static final JmxReporter jmxReporter = JmxReporter.forRegistry(JmxCounter.metricRegistry).build();
    final Logger logger = LoggerFactory.getLogger(NewsReaderService.class);

    private BlockingQueue<Item> queue;
    private ChannelDAO channelDAO;
    private ItemDAO itemDAO;
    private Persister persister;
    private boolean started;
    private ScheduledExecutorService executorService;

    private int corePoolSize;
    private int executorServicePeriod;
    private int updaterPeriod;

    public NewsReaderService() throws IOException {
        Engine.getOutput().show("Creating NewsReaderService...");
        queue = new LinkedBlockingQueue<>();
        channelDAO = new HBaseChannelDAO();
        itemDAO = new ItemDAOImpl(queue);
        persister = new Persister(queue);
        started = false;

        corePoolSize = Integer.parseInt(Engine.getConfigs().get("newsReader.corePoolSize"));
        executorService = Executors.newScheduledThreadPool(corePoolSize, r -> {
            Thread thread = Executors.defaultThreadFactory().newThread(r);
            thread.setDaemon(false);
            return thread;
        });

        executorServicePeriod = Integer.parseInt(Engine.getConfigs().get("newsReader.executorServicePeriod"));
        updaterPeriod = Integer.parseInt(Engine.getConfigs().get("newsReader.updaterPeriod"));

        Engine.getOutput().show("NewsReaderService Created Successfully...");
    }

    public static void main(String[] args) throws Exception {
        Engine.start(new ConsoleOutput(),new SystemConfigs("newsreader"));
        Engine.getInstance().startService(new NewsReaderService());
    }

    public void setChannelDAO(ChannelDAO channelDAO) {
        this.channelDAO = channelDAO;
    }

    public void crawl(URL rssLink) {

        executorService.schedule(new SiteCrawler(rssLink, channelDAO, itemDAO), 0, TimeUnit.NANOSECONDS);
        logger.debug("{} scheduled in executor service!", rssLink);
    }

    @Override
    public synchronized void start() {
        if (started) {
            throw new IllegalStateException("Already Started");
        }

        started = true;
        executorService.scheduleAtFixedRate(new UpdaterThread(), 0, executorServicePeriod, TimeUnit.SECONDS);
        executorService.schedule(persister, 0, TimeUnit.SECONDS);
        jmxReporter.start();
    }

    @Override
    public Map<String, Object> status() {
        return null;
    }

    @Override
    public String getName() {
        return "newsReader";
    }

    @Override
    public void stop() {
        Engine.getOutput().show("Stopping NewsReaderService");
        jmxReporter.stop();
        executorService.shutdownNow();
        try {
            Engine.getOutput().show("Waiting Five Second To Stop Threads....");
            executorService.awaitTermination(5, TimeUnit.SECONDS);
            channelDAO.stop();
        } catch (InterruptedException e) {
            logger.error("Service stop error: ", e);
            Engine.getOutput().show(Output.Type.ERROR, e.getMessage());
        }
    }

    private class UpdaterThread implements Runnable {
        final Logger logger = LoggerFactory.getLogger(UpdaterThread.class);

        @Override
        public void run() {
            Engine.getOutput().show("Start UpdaterThread at: " + String.valueOf(new Date()));
            try {
                channelDAO.reload();
                List<Channel> channels = channelDAO.getChannelsUpdatedBefore(updaterPeriod);
                for (Channel channel : channels) {
                    logger.info("Scheduled Channel Crawling Started for {} ", channel.getName());
                    channel.setLastUpdate(new Date().getTime());
                    channelDAO.updateChannelLastDate(channel);
                    crawl(channel.getRssLink());
                }

            } catch (Exception e) {
                logger.error("error in Channels Crawler Starter Thread", e);
                Engine.getOutput().show(Output.Type.ERROR, e.getMessage());
            }
        }

    }

}
