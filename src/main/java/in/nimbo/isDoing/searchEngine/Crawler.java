package in.nimbo.isDoing.searchEngine;

import in.nimbo.isDoing.searchEngine.elastic.ElasticSearchDAOImpl;
import in.nimbo.isDoing.searchEngine.interfaces.UrlQueue;
import in.nimbo.isDoing.searchEngine.interfaces.searchDAO;
import in.nimbo.isDoing.searchEngine.kafka.KafkaUrlQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Crawler {
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    private UrlQueue<String> queue;
    private LRU lru = new LRU();
    private searchDAO dao = new ElasticSearchDAOImpl();

    public Crawler(UrlQueue<String> queue) {
        this.queue = queue;
    }

    public Crawler() {
        this(new KafkaUrlQueue());
    }

    public void start() {
        loadQueueSeeds();
        while (true) {
            List<String> pop = queue.pop(50);
            for (String url : pop) {
                try {
                    new SiteCrawler(new URL(url), queue, dao, lru).run();
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public void loadQueueSeeds() {
        try {
            Path seedLock = Paths.get("./seed.lock");
            Path seedFile = Paths.get("./seeds.txt");
            if (/*!Files.exists(seedLock) &&*/ Files.exists(seedFile)) {
//                Files.createFile(seedLock);
                List<String> lines = Files.readAllLines(seedFile);
                for (String line : lines) {
                    queue.push(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
