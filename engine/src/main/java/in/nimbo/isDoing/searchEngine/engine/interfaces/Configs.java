package in.nimbo.isDoing.searchEngine.engine.interfaces;

import java.nio.file.Path;

public interface Configs {
    String get(String key);

    String get(String key, String value);

    Path getLoadedPath();
}
