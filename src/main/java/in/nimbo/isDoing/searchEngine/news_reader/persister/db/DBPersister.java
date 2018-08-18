package in.nimbo.isDoing.searchEngine.news_reader.persister.db;

import in.nimbo.isDoing.searchEngine.news_reader.model.Item;

public interface DBPersister {
    void persist(Item item) throws Exception;

    void flush() throws Exception;
}
