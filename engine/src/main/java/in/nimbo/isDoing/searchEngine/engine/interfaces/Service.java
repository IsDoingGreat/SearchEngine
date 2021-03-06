package in.nimbo.isDoing.searchEngine.engine.interfaces;

import java.util.Map;

public interface Service extends Stateful {
    void start();

    void stop();

    Map<String, Object> status();

    String getName();
}
