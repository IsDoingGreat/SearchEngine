package in.nimbo.isDoing.searchEngine.engine.interfaces;

import in.nimbo.isDoing.searchEngine.engine.Status;

public interface Service extends HaveStatus {
    void start();

    void stop();

    Status status();

    String getName();
}
