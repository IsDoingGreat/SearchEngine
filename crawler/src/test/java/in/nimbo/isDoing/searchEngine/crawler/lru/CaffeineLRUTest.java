package in.nimbo.isDoing.searchEngine.crawler.lru;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Configs;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CaffeineLRUTest {

    @Before
    public void setup() throws Exception {
        Engine.start(new Output() {
            @Override
            public void show(String object) {
            }

            @Override
            public void show(Type type, String object) {
            }

            @Override
            public void show(Status status) {
            }
        }, new Configs() {
            private Properties testConfig = new Properties();
            private Path testConfigPath = Paths.get("./testConfigs.properties");

            {
                testConfig.load(new FileInputStream(testConfigPath.toFile()));
            }

            @Override
            public String get(String key) {
                return testConfig.getProperty(key);
            }

            @Override
            public String get(String key, String value) {
                return testConfig.getProperty(key, value);
            }
        });
    }

    @After
    public void shutdown(){
        Engine.shutdown();
    }

    @Test
    public void isRecentlyUsed() throws InterruptedException {
        LRU lru = new CaffeineLRU();
        assertFalse(lru.isRecentlyUsed("https://quera.ir/"));
        lru.setUsed("https://quera.ir/");
        assertTrue(lru.isRecentlyUsed("https://quera.ir/"));
        int expireSeconds = Integer.parseInt(Engine.getConfigs().get("crawler.lru.caffeine.expireSeconds", "1"));
        TimeUnit.SECONDS.sleep(expireSeconds);
        assertFalse(lru.isRecentlyUsed("https://quera.ir/"));
    }

    @Test
    public void setUsed() {
        LRU lru = new CaffeineLRU();
        assertFalse(lru.isRecentlyUsed("https://quera.ir/"));
        lru.setUsed("https://quera.ir/");
        assertTrue(lru.isRecentlyUsed("https://quera.ir/"));
    }
}