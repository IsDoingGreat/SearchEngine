package in.nimbo.isDoing.searchEngine.news_reader.dao;

import in.nimbo.isDoing.searchEngine.news_reader.model.Item;

public interface ItemDAO {
    boolean checkItemExists(Item item) throws Exception;

    void insertItem(Item item) throws Exception;
}
