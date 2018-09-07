package in.nimbo.isDoing.searchEngine.engine.interfaces;

import java.nio.file.Path;
import java.util.Properties;

public interface Configs {
    void load() throws Exception;

    String get(String key);

    String get(String key, String value);

    Path getLoadedPath();

    Properties getMap();
}
