package in.nimbo.isDoing.searchEngine.crawler.lru;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LRULinkHashMapImplTest {
    @Before
    public void setup() throws Exception {
        Engine.start(new Output() {
            @Override
            public void show(String object) {
            }

            @Override
            public void show(Type type, String object) {
            }

        });
    }

    @After
    public void shutdown(){
        Engine.shutdown();
    }

    @Test
    public void isRecentlyUsedTrueForm() {
        LRU lru = new LRULinkHashMapImpl();
        lru.setUsed("http://example.com/");
        lru.setUsed("https://www.google.com/");
        assertTrue(lru.isRecentlyUsed("http://example.com/"));
    }

    @Test
    public void isRecentlyUsedFalseForm() {
        LRU lru = new LRULinkHashMapImpl();
        lru.setUsed("http://example.com/");
        lru.setUsed("https://www.google.com/");
        assertFalse(lru.isRecentlyUsed("https://quera.ir/"));
    }

    @Test
    public void setUsed() {
        LRU lru = new LRULinkHashMapImpl();
        assertFalse(lru.isRecentlyUsed("https://quera.ir/"));
        lru.setUsed("https://quera.ir/");
        assertTrue(lru.isRecentlyUsed("https://quera.ir/"));
    }
}