package in.nimbo.isDoing.searchEngine.crawler.persister.db;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;

public interface DBPersister {
    void persist(Page page) throws Exception;
    void flush() throws Exception;
}
