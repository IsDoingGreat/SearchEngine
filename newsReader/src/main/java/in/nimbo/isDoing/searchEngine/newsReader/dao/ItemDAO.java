package in.nimbo.isDoing.searchEngine.newsReader.dao;

import in.nimbo.isDoing.searchEngine.newsReader.model.Item;

public interface ItemDAO {
    boolean checkItemExists(Item item) throws Exception;

    void insertItem(Item item) throws Exception;
}
