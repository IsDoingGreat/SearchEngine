package in.nimbo.isDoing.searchEngine.newsReader.persister.db;

import in.nimbo.isDoing.searchEngine.newsReader.model.Item;

public interface DBPersister {
    void persist(Item item) throws Exception;

    void flush() throws Exception;
}
